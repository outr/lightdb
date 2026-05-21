package lightdb.api

import fabric.*

/**
 * Body of an [[ApiResponse]]. Three shapes:
 *
 *   - [[ApiContent.Empty]]   — no body (e.g. 204).
 *   - [[ApiContent.JsonValue]] — a single fabric.Json value, serialized at the
 *     binding layer with `application/json`.
 *   - [[ApiContent.Stream]]  — a stream of fabric.Json values, intended to be
 *     emitted as a Server-Sent Events response (`text/event-stream`). One JSON
 *     value per SSE event.
 */
sealed trait ApiContent

object ApiContent {
  case object Empty extends ApiContent
  case class JsonValue(value: Json) extends ApiContent
  case class Stream(events: rapid.Stream[Json]) extends ApiContent

  private[api] def errorJson(message: String): Json = obj("error" -> str(message))
}
