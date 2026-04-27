package lightdb.query

/**
 * Cross-backend highlight request.
 *
 * Maps directly to OpenSearch/Lucene highlight semantics: per-field fragment counts, fragment
 * size, and pre/post tags. Only OpenSearch wires this through end-to-end today; other backends
 * accept the spec and return empty highlights in the result.
 *
 * @param fields            list of field names to highlight; empty means "all fields in `_source`"
 * @param preTag            HTML/marker prefix wrapped around each match (default `<em>`)
 * @param postTag           closing marker (default `</em>`)
 * @param fragmentSize      max length per fragment in characters (None = backend default)
 * @param numberOfFragments how many fragments per field (None = backend default)
 * @param requireFieldMatch if true (default), only highlight fields that participated in the
 *                          query — set false to highlight any field matching one of the query
 *                          terms
 */
case class HighlightSpec(fields: List[String] = Nil,
                         preTag: String = "<em>",
                         postTag: String = "</em>",
                         fragmentSize: Option[Int] = None,
                         numberOfFragments: Option[Int] = None,
                         requireFieldMatch: Boolean = true)
