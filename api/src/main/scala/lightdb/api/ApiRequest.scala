package lightdb.api

import fabric.Json

/**
 * Framework-agnostic representation of an inbound API request.
 *
 * The `method` is uppercase ("GET" / "POST" / etc.). `path` is the request path
 * relative to the API root (no leading or trailing slash significance — the
 * handler tolerates both). `params` is a flat map of query-string parameters;
 * repeated keys collapse to the first value. `body` is the request body parsed
 * as JSON if present, otherwise `None`.
 */
case class ApiRequest(method: String,
                      path: String,
                      params: Map[String, String] = Map.empty,
                      body: Option[Json] = None)
