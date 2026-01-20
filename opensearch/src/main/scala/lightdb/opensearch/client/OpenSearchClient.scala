package lightdb.opensearch.client

import fabric._
import fabric.io.{JsonFormatter, JsonParser}
import rapid.Task
import rapid.taskTaskOps
import spice.http.{Header, HeaderKey, Headers, HttpMethod, HttpResponse, HttpStatus}
import spice.http.client.HttpClient
import spice.http.content.Content
import spice.net.{ContentType, URL}

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.util.{Failure, Success}

import lightdb.opensearch.OpenSearchMetrics

/**
 * OpenSearch HTTP client wrapper used by the OpenSearch store/transaction.
 */
case class OpenSearchClient(config: OpenSearchConfig) {
  private def url(path: String): URL = URL.parse(s"${config.normalizedBaseUrl}$path")

  if (config.metricsEnabled) {
    config.metricsLogEvery.foreach(every => OpenSearchMetrics.startPeriodicLogging(config.normalizedBaseUrl, every))
  }

  private lazy val client: HttpClient = HttpClient
    .headers(Headers().withHeaders(List(
      config.authHeader.map(auth => Headers.Request.Authorization(auth)),
      config.opaqueId.map(id => Header(HeaderKey("X-Opaque-Id"), id))
    ).flatten: _*))
    // We do our own status handling so that 404s can be treated as "not found" where appropriate.
    .noFailOnHttpStatus
    .timeout(config.requestTimeout)
    .url(URL.parse(config.normalizedBaseUrl))

  private def readBody(response: HttpResponse): Task[Option[String]] = response.content match {
    case Some(c) => c.asString.map(Some(_))
    case None => Task.pure(None)
  }

  private def log(method: HttpMethod, u: URL, status: HttpStatus, tookMs: Long): Unit = {
    if (config.logRequests) {
      val path = u.path.decoded
      val excluded = config.logRequestsExcludePaths.exists(p => p.nonEmpty && path.contains(p))
      if (!excluded) {
        scribe.info(s"OpenSearch ${method.value} $path -> ${status.code} (${tookMs}ms)")
      }
    }
  }

  private def opName(req: HttpClient): String =
    s"${req.method.value} ${req.url.path.decoded}"

  private def shouldRetry(statusCode: Int): Boolean =
    config.retryStatusCodes.contains(statusCode)

  private def jitterDelay(delayMs: Long): Long = {
    // jitter in [0.5x, 1.5x]
    val factor = ThreadLocalRandom.current().nextDouble(0.5, 1.5)
    math.max(0L, (delayMs.toDouble * factor).toLong)
  }

  private def send(req: HttpClient): Task[HttpResponse] =
    sendWithRetry(req, name = opName(req))

  /**
   * Lightweight connectivity check to distinguish a slow query from an unreachable OpenSearch node.
   *
   * Uses a short per-request timeout regardless of the configured requestTimeout.
   */
  def ping(timeout: FiniteDuration = 2.seconds): Task[Boolean] = Task.defer {
    val req = client
      .get
      .modifyUrl(_.withPath("/"))
      .timeout(timeout)
    req.send().attempt.map {
      case Success(resp) => resp.status.isSuccess
      case Failure(_) => false
    }
  }

  private def sendWithRetry(req: HttpClient, name: String): Task[HttpResponse] = Task.defer {
    val maxAttempts = math.max(1, config.retryMaxAttempts)
    val initialDelayMs = math.max(0L, config.retryInitialDelay.toMillis)
    val maxDelayMs = math.max(0L, config.retryMaxDelay.toMillis)

    def nextDelayMs(current: Long): Long =
      math.min(maxDelayMs, math.max(0L, current * 2L))

    def attempt(n: Int, delayMs: Long): Task[HttpResponse] = Task.defer {
      val started = System.nanoTime()
      val completed = new AtomicBoolean(false)

      def elapsedMs: Long = (System.nanoTime() - started) / 1000000L

      def slowLogLoop(afterMs: Long): Task[Unit] = {
        if (afterMs <= 0L) {
          Task.unit
        } else {
          Task.sleep(afterMs.millis).next {
            Task.defer {
              if (!completed.get()) {
                // Check if OpenSearch is reachable right now (short timeout) to disambiguate "slow" vs "down".
                ping(timeout = 2.seconds).map { ok =>
                  val health = if (ok) "reachable" else "unreachable"
                  scribe.warn(
                    s"OpenSearch request still running after ${elapsedMs}ms: $name attempt=$n/$maxAttempts timeoutMs=${config.requestTimeout.toMillis} opensearch=$health"
                  )
                }
              } else {
                Task.unit
              }
            }.next {
              if (!completed.get()) {
                config.slowRequestLogEvery match {
                  case Some(every) => slowLogLoop(every.toMillis)
                  case None => Task.unit
                }
              } else {
                Task.unit
              }
            }
          }
        }
      }

      // Fire-and-forget watchdog to log long-running requests.
      config.slowRequestLogAfter.foreach { after =>
        slowLogLoop(after.toMillis).start()
      }

      req
        .send()
        .guarantee(Task(completed.set(true)))
        .map { resp =>
        val tookMs = (System.nanoTime() - started) / 1000000L
        log(req.method, req.url, resp.status, tookMs)
        if (config.metricsEnabled) {
          OpenSearchMetrics.recordRequest(config.normalizedBaseUrl, tookMs)
        }
        resp
      }.attempt.flatMap {
        case Success(resp) =>
          val retryable = shouldRetry(resp.status.code)
          if (config.metricsEnabled && !resp.status.isSuccess && (!retryable || n >= maxAttempts)) {
            OpenSearchMetrics.recordFailure(config.normalizedBaseUrl)
          }
          if (retryable && n < maxAttempts) {
            val sleepMs = jitterDelay(delayMs)
            if (config.logRequests) {
              scribe.warn(s"OpenSearch retrying $name after status=${resp.status.code} attempt=$n/$maxAttempts sleepMs=$sleepMs")
            }
            if (config.metricsEnabled) {
              OpenSearchMetrics.recordRetry(config.normalizedBaseUrl)
            }
            Task.sleep(sleepMs.millis).next(attempt(n + 1, nextDelayMs(delayMs)))
          } else {
            Task.pure(resp)
          }
        case Failure(t) =>
          if (n < maxAttempts) {
            val sleepMs = jitterDelay(delayMs)
            if (config.logRequests) {
              scribe.warn(s"OpenSearch retrying $name after exception attempt=$n/$maxAttempts sleepMs=$sleepMs (${t.getClass.getSimpleName}: ${t.getMessage})")
            }
            if (config.metricsEnabled) {
              OpenSearchMetrics.recordRetry(config.normalizedBaseUrl)
            }
            Task.sleep(sleepMs.millis).next(attempt(n + 1, nextDelayMs(delayMs)))
          } else {
            if (config.metricsEnabled) {
              OpenSearchMetrics.recordFailure(config.normalizedBaseUrl)
            }
            Task.error(t)
          }
      }
    }

    attempt(n = 1, delayMs = initialDelayMs)
  }

  private def require2xx(name: String, resp: HttpResponse): Task[Unit] =
    if (resp.status.isSuccess) Task.unit
    else readBody(resp).flatMap(b => Task.error(new RuntimeException(s"OpenSearch $name failed (${resp.status.code}): ${b.getOrElse("")}")))

  def indexExists(index: String): Task[Boolean] =
    send(client.method(HttpMethod.Head).url(url(s"/$index"))).flatMap { resp =>
      resp.status.code match {
        case 200 => Task.pure(true)
        case 404 => Task.pure(false)
        case _ => require2xx(s"indexExists($index)", resp).map(_ => true)
      }
    }

  def aliasExists(alias: String): Task[Boolean] =
    send(client.method(HttpMethod.Head).url(url(s"/_alias/${escapePath(alias)}"))).flatMap { resp =>
      resp.status.code match {
        case 200 => Task.pure(true)
        case 404 => Task.pure(false)
        case _ => require2xx(s"aliasExists($alias)", resp).map(_ => true)
      }
    }

  def aliasTargets(alias: String): Task[List[String]] = {
    val req = client.get.url(url(s"/_alias/${escapePath(alias)}"))
    send(req).flatMap { resp =>
      resp.status.code match {
        case 200 =>
          readBody(resp).map {
            case Some(body) => JsonParser(body).asObj.value.keys.toList.sorted
            case None => Nil
          }
        case 404 => Task.pure(Nil)
        case _ => require2xx(s"aliasTargets($alias)", resp).map(_ => Nil)
      }
    }
  }

  def mappingHash(index: String): Task[Option[String]] = {
    val req = client.get.url(url(s"/$index/_mapping"))
    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap { b =>
          Task.error(new RuntimeException(s"OpenSearch mappingHash failed (${resp.status.code}) for $index: ${b.getOrElse("")}"))
        }
      } else {
        readBody(resp).map(_.getOrElse("{}")).map(JsonParser(_)).map { json =>
          // Response format: { "<index>": { "mappings": { ... } } }
          val byIndex = json.asObj.value
          byIndex.headOption.flatMap { case (_, idxObj) =>
            idxObj.asObj.get("mappings").flatMap(_.asObj.get("_meta")).flatMap(_.asObj.get("lightdb")).flatMap(_.asObj.get("mapping_hash")).map(_.asString)
          }
        }
      }
    }
  }

  def createIndex(index: String, body: Json): Task[Unit] = {
    val req = client
      .method(HttpMethod.Put)
      .header(Headers.`Content-Type`(ContentType.`application/json`))
      .url(url(s"/$index"))
      .json(body)
    send(req).flatMap(resp => require2xx(s"createIndex($index)", resp))
  }

  def deleteIndex(index: String): Task[Unit] = {
    val req = client.method(HttpMethod.Delete).url(url(s"/$index"))
    send(req).flatMap { resp =>
      if (resp.status.code == 404) Task.unit else require2xx(s"deleteIndex($index)", resp)
    }
  }

  /**
   * Force refresh so recent writes become searchable.
   *
   * IMPORTANT:
   * OpenSearch may return HTTP 200 while still reporting shard failures in the body.
   * To honor LightDB's "commit means searchable" contract, we validate the `_shards` response and
   * retry briefly if refresh was only partially successful.
   */
  def refreshIndex(index: String): Task[Unit] = {
    val maxAttempts = 6
    val initialDelayMs = 25L
    val maxDelayMs = 1000L

    def validateRefreshBody(body: String): Task[Unit] = Task {
      // Typical response:
      // { "_shards": { "total": 2, "successful": 1, "failed": 0 } }
      val json = JsonParser(body)
      val shards = json.asObj.get("_shards").map(_.asObj).getOrElse(obj().asObj)
      val total = shards.get("total").map(_.asInt).getOrElse(0)
      val successful = shards.get("successful").map(_.asInt).getOrElse(0)
      val failed = shards.get("failed").map(_.asInt).getOrElse(0)

      // If OpenSearch omits shard info, assume success (best effort) but keep a warning for visibility.
      if (total == 0 && successful == 0 && failed == 0) {
        scribe.warn(s"OpenSearch refreshIndex($index) returned body without _shards stats; assuming success. body=${body.take(512)}")
      } else if (failed > 0) {
        throw new RuntimeException(
          s"OpenSearch refreshIndex($index) incomplete: total=$total successful=$successful failed=$failed body=${body.take(512)}"
        )
      } else if (total > 0 && successful <= 0) {
        // Extremely unlikely, but treat as failure because refresh had no acknowledged shards.
        throw new RuntimeException(
          s"OpenSearch refreshIndex($index) reported successful=0 (total=$total failed=$failed) body=${body.take(512)}"
        )
      } else if (total > 0 && successful != total) {
        // Replica shards may be unassigned in single-node clusters (common in tests), resulting in successful < total.
        // This does not break read-after-write on primary shards, so warn but do not fail.
        scribe.warn(
          s"OpenSearch refreshIndex($index) partial success (likely unassigned replicas): total=$total successful=$successful failed=$failed"
        )
      }
    }

    def nextDelayMs(current: Long): Long =
      math.min(maxDelayMs, math.max(0L, current * 2L))

    def attempt(n: Int, delayMs: Long): Task[Unit] = Task.defer {
      val req = client.method(HttpMethod.Post).url(url(s"/$index/_refresh"))
      send(req).flatMap { resp =>
        require2xx(s"refreshIndex($index)", resp).next {
          readBody(resp).flatMap {
            case Some(body) =>
              validateRefreshBody(body).attempt.flatMap {
                case Success(_) => Task.unit
                case Failure(t) if n < maxAttempts =>
                  // If refresh was partial, backoff + retry. This usually indicates shard not ready immediately after truncate/reindex.
                  val sleepMs = jitterDelay(delayMs)
                  if (config.logRequests) {
                    scribe.warn(s"OpenSearch refreshIndex retrying index=$index attempt=$n/$maxAttempts sleepMs=$sleepMs (${t.getClass.getSimpleName}: ${t.getMessage})")
                  }
                  Task.sleep(sleepMs.millis).next(attempt(n + 1, nextDelayMs(delayMs)))
                case Failure(t) =>
                  Task.error(t)
              }
            case None =>
              // No body; we can't validate shard status. Treat as success.
              Task.unit
          }
        }
      }
    }

    attempt(n = 1, delayMs = initialDelayMs)
  }

  def indexSettings(index: String,
                    flatSettings: Boolean = true,
                    includeDefaults: Boolean = true): Task[Json] = {
    val req = client
      .get
      .modifyUrl { u =>
        u.withPath(s"/$index/_settings")
          .withParam("flat_settings", if (flatSettings) "true" else "false")
          .withParam("include_defaults", if (includeDefaults) "true" else "false")
      }
    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap(b => Task.error(new RuntimeException(s"OpenSearch indexSettings failed (${resp.status.code}) for $index: ${b.getOrElse("")}")))
      } else {
        readBody(resp).map(_.getOrElse("{}")).map(JsonParser(_))
      }
    }
  }

  def updateIndexSettings(index: String, settings: Json): Task[Unit] = {
    val req = client
      .method(HttpMethod.Put)
      .header(Headers.`Content-Type`(ContentType.`application/json`))
      .modifyUrl(_.withPath(s"/$index/_settings"))
      .json(settings)
    send(req).flatMap(resp => require2xx(s"updateIndexSettings($index)", resp))
  }

  def indexDoc(index: String,
               id: String,
               source: Json,
               refresh: Option[String] = None,
               routing: Option[String] = None): Task[Unit] = {
    val req = client
      .method(HttpMethod.Put)
      .header(Headers.`Content-Type`(ContentType.`application/json`))
      .modifyUrl { u =>
        u.withPath(s"/$index/_doc/${escapePath(id)}")
          .withParamOpt("refresh", refresh)
          .withParamOpt("routing", routing.map(escapePath))
      }
      .json(source)
    send(req).flatMap(resp => require2xx(s"indexDoc($index,$id)", resp))
  }

  def bulk(bodyNdjson: String, refresh: Option[String]): Task[Unit] = {
    bulkResponse(bodyNdjson, refresh).flatMap { json =>
      val errors = json.asObj.get("errors").exists(_.asBoolean)
      if (!errors) Task.unit
      else {
        val items = json.asObj.get("items").map(_.asArr.value.toList).getOrElse(Nil)
        val firstError = items.iterator.flatMap { j =>
          j.asObj.value.valuesIterator.flatMap(_.asObj.get("error")).toList
        }.take(1).toList.headOption
        val msg = firstError.map(e => s" firstError=${JsonFormatter.Compact(e)}").getOrElse("")
        Task.error(new RuntimeException(s"OpenSearch bulk reported errors=true.$msg"))
      }
    }
  }

  def bulkResponse(bodyNdjson: String, refresh: Option[String]): Task[Json] = {
    val req = client
      .method(HttpMethod.Post)
      .modifyUrl { u =>
        u.withPath("/_bulk").withParamOpt("refresh", refresh)
      }
      // Spice's ContentType renderer currently produces an invalid header for x-ndjson
      // (ex: `application/x-ndjson/ndjson`). Set the header explicitly.
      .header(Header(HeaderKey("Content-Type"), "application/x-ndjson"))
      .content(Content.string(bodyNdjson, ContentType.`application/json`))

    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap { b =>
          if (config.logRequests) {
            scribe.error(s"OpenSearch bulk failed (${resp.status.code}): ${b.getOrElse("")}")
          }
          Task.error(new RuntimeException(s"OpenSearch bulk failed (${resp.status.code}): ${b.getOrElse("")}"))
        }
      } else {
        readBody(resp).map(_.getOrElse("{}")).map(JsonParser(_))
      }
    }
  }

  def search(index: String, body: Json, filterPathOverride: Option[String] = None): Task[Json] = {
    val req = client
      .post
      .header(Headers.`Content-Type`(ContentType.`application/json`))
      .modifyUrl { u =>
        val filterPath = filterPathOverride.orElse(config.searchFilterPath)
        u.withPath(s"/$index/_search").withParamOpt("filter_path", filterPath)
      }
      .json(body)
    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap(b => Task.error(new RuntimeException(s"OpenSearch search failed (${resp.status.code}) for $index: ${b.getOrElse("")}")))
      } else {
        readBody(resp).map(_.getOrElse("{}")).map(JsonParser(_))
      }
    }
  }

  def getDoc(index: String, id: String): Task[Option[Json]] = {
    val req = client.get.url(url(s"/$index/_doc/${escapePath(id)}"))
    send(req).flatMap { resp =>
      resp.status.code match {
        case 200 =>
          readBody(resp).map(_.getOrElse("{}")).map(JsonParser(_)).map(_.get("_source"))
        case 404 =>
          // index missing OR doc missing: both are "not found"
          Task.pure(None)
        case _ =>
          readBody(resp).flatMap(b => Task.error(new RuntimeException(s"OpenSearch getDoc failed (${resp.status.code}) for $index/$id: ${b.getOrElse("")}")))
      }
    }
  }

  def deleteDoc(index: String, id: String, refresh: Option[String]): Task[Boolean] = {
    val req = client.method(HttpMethod.Delete).modifyUrl { u =>
      u.withPath(s"/$index/_doc/${escapePath(id)}").withParamOpt("refresh", refresh)
    }
    send(req).flatMap { resp =>
      resp.status.code match {
        case 200 | 202 => Task.pure(true)
        case 404 => Task.pure(false)
        case _ => readBody(resp).flatMap(b => Task.error(new RuntimeException(s"OpenSearch deleteDoc failed (${resp.status.code}) for $index/$id: ${b.getOrElse("")}")))
      }
    }
  }

  def count(index: String, query: Json): Task[Int] = {
    val req = client
      .post
      .header(Headers.`Content-Type`(ContentType.`application/json`))
      .url(url(s"/$index/_count"))
      .json(query)
    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap(b => Task.error(new RuntimeException(s"OpenSearch count failed (${resp.status.code}) for $index: ${b.getOrElse("")}")))
      } else {
        readBody(resp).map(_.getOrElse("{}")).map(JsonParser(_)).map(_.get("count").map(_.asInt).getOrElse(0))
      }
    }
  }

  def deleteByQuery(index: String, query: Json, refresh: Option[String]): Task[Int] = {
    val req = client
      .post
      .header(Headers.`Content-Type`(ContentType.`application/json`))
      .modifyUrl { u =>
        u.withPath(s"/$index/_delete_by_query").withParamOpt("refresh", refresh)
      }
      .json(query)
    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap(b => Task.error(new RuntimeException(s"OpenSearch deleteByQuery failed (${resp.status.code}) for $index: ${b.getOrElse("")}")))
      } else {
        readBody(resp).map(_.getOrElse("{}")).map(JsonParser(_)).map(_.get("deleted").map(_.asInt).getOrElse(0))
      }
    }
  }

  def updateAliases(body: Json): Task[Unit] = {
    val req = client
      .post
      .header(Headers.`Content-Type`(ContentType.`application/json`))
      .url(url("/_aliases"))
      .json(body)
    send(req).flatMap(resp => require2xx("updateAliases", resp))
  }

  /**
   * Reindex documents from a source index/alias into a destination index.
   *
   * Note: this is best-effort and intentionally minimal; callers should treat this as an offline migration helper
   * unless they also coordinate writes (e.g. pause writes or dual-write).
   */
  def reindex(source: String,
              dest: String,
              refresh: Boolean = true,
              waitForCompletion: Boolean = true): Task[Unit] = {
    val body = obj(
      "source" -> obj("index" -> str(source)),
      "dest" -> obj("index" -> str(dest))
    )
    val req = client
      .post
      .header(Headers.`Content-Type`(ContentType.`application/json`))
      .modifyUrl { u =>
        u.withPath("/_reindex")
          .withParam("refresh", if (refresh) "true" else "false")
          .withParam("wait_for_completion", if (waitForCompletion) "true" else "false")
      }
      .json(body)
    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap(b => Task.error(new RuntimeException(s"OpenSearch reindex failed (${resp.status.code}) $source -> $dest: ${b.getOrElse("")}")))
      } else {
        // Response contains summary; we only validate that we didn't get an error.
        Task.unit
      }
    }
  }

  private def escapePath(s: String): String =
    java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8)
}


