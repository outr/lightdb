package lightdb.opensearch.util

import fabric._
import fabric.io.{JsonFormatter, JsonParser}

import java.util.Base64
import scala.util.Try

/**
 * Encodes/decodes OpenSearch `search_after` values (a JSON array) into an opaque, URL-safe token.
 */
object OpenSearchCursor {
  def encode(searchAfter: Json): String = {
    val s = JsonFormatter.Compact(searchAfter)
    Base64.getUrlEncoder.withoutPadding().encodeToString(s.getBytes("UTF-8"))
  }

  def decode(token: String): Option[Json] =
    Try {
      val bytes = Base64.getUrlDecoder.decode(token)
      JsonParser(new String(bytes, "UTF-8"))
    }.toOption
}



