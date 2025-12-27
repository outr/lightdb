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
import scala.concurrent.duration.DurationLong
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
      scribe.info(s"OpenSearch ${method.value} ${u.path.decoded} -> ${status.code} (${tookMs}ms)")
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

  private def sendWithRetry(req: HttpClient, name: String): Task[HttpResponse] = Task.defer {
    val maxAttempts = math.max(1, config.retryMaxAttempts)
    val initialDelayMs = math.max(0L, config.retryInitialDelay.toMillis)
    val maxDelayMs = math.max(0L, config.retryMaxDelay.toMillis)

    def nextDelayMs(current: Long): Long =
      math.min(maxDelayMs, math.max(0L, current * 2L))

    def attempt(n: Int, delayMs: Long): Task[HttpResponse] = Task.defer {
      val started = System.nanoTime()
      req.send().map { resp =>
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

  def refreshIndex(index: String): Task[Unit] = {
    val req = client.method(HttpMethod.Post).url(url(s"/$index/_refresh"))
    send(req).flatMap(resp => require2xx(s"refreshIndex($index)", resp))
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


