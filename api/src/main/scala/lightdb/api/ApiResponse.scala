package lightdb.api

import fabric.Json

/**
 * Framework-agnostic representation of an outbound API response.
 *
 * `status` is an HTTP-style integer status. `content` describes the body shape
 * (empty / json / SSE-style stream). `headers` is an optional bag of additional
 * response headers — the framework binding decides how to merge these with any
 * content-type headers it sets automatically.
 */
case class ApiResponse(status: Int,
                       content: ApiContent = ApiContent.Empty,
                       headers: Map[String, String] = Map.empty)

object ApiResponse {
  def ok(content: ApiContent): ApiResponse = ApiResponse(200, content)
  def ok(json: Json): ApiResponse = ApiResponse(200, ApiContent.JsonValue(json))
  def noContent: ApiResponse = ApiResponse(204, ApiContent.Empty)
  def notFound(message: String = "Not Found"): ApiResponse =
    ApiResponse(404, ApiContent.JsonValue(ApiContent.errorJson(message)))
  def badRequest(message: String): ApiResponse =
    ApiResponse(400, ApiContent.JsonValue(ApiContent.errorJson(message)))
  def methodNotAllowed(message: String = "Method Not Allowed"): ApiResponse =
    ApiResponse(405, ApiContent.JsonValue(ApiContent.errorJson(message)))
  def conflict(message: String): ApiResponse =
    ApiResponse(409, ApiContent.JsonValue(ApiContent.errorJson(message)))
  def serverError(message: String): ApiResponse =
    ApiResponse(500, ApiContent.JsonValue(ApiContent.errorJson(message)))
}
