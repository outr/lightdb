package lightdb.opensearch

import lightdb.doc.{Document, DocumentModel}
import lightdb.{SearchResults}

/**
 * A single cursor-pagination page result for OpenSearch `search_after`.
 */
case class OpenSearchCursorPage[Doc <: Document[Doc], Model <: DocumentModel[Doc], V](
  results: SearchResults[Doc, Model, V],
  nextCursorToken: Option[String]
)



