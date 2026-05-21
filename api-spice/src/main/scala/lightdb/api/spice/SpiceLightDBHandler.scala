package lightdb.api.spice

import fabric.io.{JsonFormatter, JsonParser}
import lightdb.api.{ApiContent, ApiRequest, ApiResponse, LightDBHttpHandler}
import rapid.*
import scribe.mdc.MDC
import spice.http.content.{Content, JsonContent, SSEContent, StringContent}
import spice.http.server.MutableHttpServer
import spice.http.server.handler.HttpHandler
import spice.http.{HttpExchange, HttpStatus}
import spice.net.ContentType

import scala.util.Try

/**
 * Spice adapter that delegates inbound HTTP requests to a
 * [[LightDBHttpHandler]] and serializes the resulting [[ApiResponse]] back to
 * an [[HttpExchange]].
 *
 * Two ways to install it:
 *
 *   1. As a Spice handler in any [[MutableHttpServer]]:
 *      {{{
 *        new SpiceLightDBHandler(handler).install(server)
 *      }}}
 *
 *   2. Or by reusing this object as a handler directly (it already implements
 *      [[HttpHandler]]) — register it via `server.handlers += this`.
 *
 * The adapter wraps SSE-typed [[ApiContent.Stream]] responses in
 * [[SSEContent]] so each emitted JSON value becomes one `data: …\n\n` event.
 */
class SpiceLightDBHandler(val handler: LightDBHttpHandler) extends HttpHandler {

  override def handle(exchange: HttpExchange)(using mdc: MDC): Task[HttpExchange] = {
    val req = exchange.request
    val params = req.url.parameters.entries.iterator.map { case (k, p) => k -> p.value }.toMap

    val bodyTask: Task[Option[fabric.Json]] = req.content match {
      case Some(content) => content.asString.map { s =>
        if (s.trim.isEmpty) None else Try(JsonParser(s)).toOption
      }
      case None => Task.pure(None)
    }

    bodyTask
      .flatMap { body =>
        handler.handle(ApiRequest(
          method = req.method.value,
          path   = req.url.path.encoded,
          params = params,
          body   = body
        ))
      }
      .flatMap(resp => exchange.modify(_ => Task.pure(toSpice(resp))))
      .map(_.finish())
  }

  /** Register this adapter against a [[MutableHttpServer]]. Returns the handler
   * so callers can keep a reference for ordering / removal. */
  def install(server: MutableHttpServer): HttpHandler = {
    server.handlers += this
    this
  }

  private def toSpice(resp: ApiResponse): spice.http.HttpResponse = {
    val base = spice.http.HttpResponse().withStatus(spiceStatus(resp.status))
    val withContent = resp.content match {
      case ApiContent.Empty             => base
      case ApiContent.JsonValue(value)  => base.withContent(JsonContent(value))
      case ApiContent.Stream(events)    => base.withContent(SSEContent(
        events.map(json => s"data: ${JsonFormatter.Compact(json)}\n\n")
      ))
    }
    resp.headers.foldLeft(withContent) { case (acc, (k, v)) => acc.withHeader(k, v) }
  }

  private def spiceStatus(code: Int): HttpStatus =
    HttpStatus.getByCode(code).getOrElse(HttpStatus(code, s"Status $code"))
}

object SpiceLightDBHandler {
  def apply(handler: LightDBHttpHandler): SpiceLightDBHandler = new SpiceLightDBHandler(handler)
}
