package lightdb.opensearch.client

import fabric._
import fabric.io.JsonParser
import fabric.rw._
import rapid.Task
import spice.http.{Header, HeaderKey, Headers, HttpMethod, HttpResponse, HttpStatus}
import spice.http.client.HttpClient
import spice.http.content.Content
import spice.net.{ContentType, URL}

case class OpenSearchClient(config: OpenSearchConfig) {
  private def url(path: String): URL = {
    val base = config.normalizedBaseUrl
    val url = URL.parse(s"$base$path")
    url
  }
  private val ndjsonContentType = ContentType.parse("application/x-ndjson")

  private lazy val client: HttpClient = HttpClient
    .headers(Headers().withHeaders(List(
      config.authHeader.map(auth => Headers.Request.Authorization(auth)),
      config.opaqueId.map(id => Header(HeaderKey("X-Opaque-Id"), id))
    ).flatten: _*))
    // We do our own status handling so that 404s can be treated as "not found" where appropriate.
    // (Important for LightDB bootstrap: `_backingStore` probes before the index exists.)
    .noFailOnHttpStatus
    .url(URL.parse(config.normalizedBaseUrl))

  private def readBody(response: HttpResponse): Task[Option[String]] = response.content match {
    case Some(c) => c.asString.map(Some(_))
    case None => Task.pure(None)
  }

  private def log(method: HttpMethod, u: URL, status: HttpStatus, tookMs: Long): Unit = {
    if (config.logRequests) {
      // Keep logs small and stable: method + path + status + duration.
      scribe.info(s"OpenSearch ${method.value} ${u.path.decoded} -> ${status.code} (${tookMs}ms)")
    }
  }

  private def send(req: HttpClient): Task[HttpResponse] = Task.defer {
    val started = System.nanoTime()
    req.send().map { resp =>
      val tookMs = (System.nanoTime() - started) / 1000000L
      log(req.method, req.url, resp.status, tookMs)
      resp
    }
  }

  private def require2xx(name: String, resp: HttpResponse): Task[Unit] = {
    if (resp.status.isSuccess) {
      Task.unit
    } else {
      readBody(resp).flatMap { b =>
        Task.error(new RuntimeException(s"OpenSearch $name failed (${resp.status.code}): ${b.getOrElse("")}"))
      }
    }
  }

  def indexExists(index: String): Task[Boolean] =
    send(client.method(HttpMethod.Head).url(url(s"/$index"))).flatMap { resp =>
      resp.status.code match {
        case 200 => Task.pure(true)
        case 404 => Task.pure(false)
        case _ => require2xx(s"indexExists($index)", resp).map(_ => true) // will throw
      }
    }

  def aliasExists(alias: String): Task[Boolean] =
    send(client.method(HttpMethod.Head).url(url(s"/_alias/${escapePath(alias)}"))).flatMap { resp =>
      resp.status.code match {
        case 200 => Task.pure(true)
        case 404 => Task.pure(false)
        case _ => require2xx(s"aliasExists($alias)", resp).map(_ => true) // will throw
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
        case 404 =>
          Task.pure(Nil)
        case _ =>
          require2xx(s"aliasTargets($alias)", resp).map(_ => Nil) // will throw
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
      // deleting a missing index is idempotent
      if (resp.status.code == 404) Task.unit
      else require2xx(s"deleteIndex($index)", resp)
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
    val req = client
      .method(HttpMethod.Post)
      .modifyUrl { u =>
        u.withPath("/_bulk")
          .withParamOpt("refresh", refresh)
      }
      .header(Headers.`Content-Type`(ndjsonContentType))
      .content(Content.string(bodyNdjson, ndjsonContentType))
    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap { b =>
          Task.error(new RuntimeException(s"OpenSearch bulk failed (${resp.status.code}): ${b.getOrElse("")}"))
        }
      } else {
        // bulk response is JSON; failures may be indicated via errors=true even for 200
        readBody(resp).map(_.getOrElse("{}")).map(JsonParser(_)).flatMap { json =>
          val errors = json.asObj.get("errors").exists(_.asBoolean)
          if (!errors) {
            Task.unit
          } else {
            val items = json.asObj.get("items").map(_.asArr.value.toList).getOrElse(Nil)
            val firstError = items.iterator.flatMap { j =>
              // each item is like { "index": { "status": 201, "error": {...} } }
              j.asObj.value.valuesIterator.flatMap(_.asObj.get("error")).toList
            }.take(1).toList.headOption
            val message = firstError.map(e => s" firstError=${fabric.io.JsonFormatter.Compact(e)}").getOrElse("")
            Task.error(new RuntimeException(s"OpenSearch bulk reported errors=true.$message"))
          }
        }
      }
    }
  }

  def search(index: String, body: Json): Task[Json] = {
    val req = client
      .post
      .header(Headers.`Content-Type`(ContentType.`application/json`))
      .url(url(s"/$index/_search"))
      .json(body)
    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap { b =>
          Task.error(new RuntimeException(s"OpenSearch search failed (${resp.status.code}) for $index: ${b.getOrElse("")}"))
        }
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
          // index missing OR doc missing: both should be treated as "not found"
          Task.pure(None)
        case _ =>
          readBody(resp).flatMap { b =>
            Task.error(new RuntimeException(s"OpenSearch getDoc failed (${resp.status.code}) for $index/$id: ${b.getOrElse("")}"))
          }
      }
    }
  }

  def deleteDoc(index: String, id: String, refresh: Option[String]): Task[Boolean] = {
    val req = client.method(HttpMethod.Delete).modifyUrl { u =>
      u.withPath(s"/$index/_doc/${escapePath(id)}")
        .withParamOpt("refresh", refresh)
    }
    send(req).flatMap { resp =>
      resp.status.code match {
        case 200 | 202 => Task.pure(true)
        case 404 => Task.pure(false)
        case _ =>
          readBody(resp).flatMap { b =>
            Task.error(new RuntimeException(s"OpenSearch deleteDoc failed (${resp.status.code}) for $index/$id: ${b.getOrElse("")}"))
          }
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
        readBody(resp).flatMap { b =>
          Task.error(new RuntimeException(s"OpenSearch count failed (${resp.status.code}) for $index: ${b.getOrElse("")}"))
        }
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
        u.withPath(s"/$index/_delete_by_query")
          .withParamOpt("refresh", refresh)
      }
      .json(query)
    send(req).flatMap { resp =>
      if (!resp.status.isSuccess) {
        readBody(resp).flatMap { b =>
          Task.error(new RuntimeException(s"OpenSearch deleteByQuery failed (${resp.status.code}) for $index: ${b.getOrElse("")}"))
        }
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

  private def escapePath(s: String): String =
    java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8)
}


