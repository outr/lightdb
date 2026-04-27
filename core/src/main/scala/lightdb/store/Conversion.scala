package lightdb.store

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.materialized.{MaterializedAndDoc, MaterializedIndex}
import lightdb.query.DocWithInnerHits
import lightdb.spatial.{DistanceAndDoc, Geo, Point}

sealed trait Conversion[Doc, V]

object Conversion {
  case class Value[Doc <: Document[Doc], F](field: Field[Doc, F]) extends Conversion[Doc, F]

  case class Doc[Doc <: Document[Doc]]() extends Conversion[Doc, Doc]

  case class Json[Doc <: Document[Doc]](fields: List[Field[Doc, _]]) extends Conversion[Doc, fabric.Json]

  case class Materialized[Doc <: Document[Doc], Model <: DocumentModel[Doc]](fields: List[Field[Doc, _]]) extends Conversion[Doc, MaterializedIndex[Doc, Model]]

  case class DocAndIndexes[Doc <: Document[Doc], Model <: DocumentModel[Doc]]() extends Conversion[Doc, MaterializedAndDoc[Doc, Model]]

  case class Converted[Doc <: Document[Doc], T](f: Doc => T) extends Conversion[Doc, T]

  case class Distance[Doc <: Document[Doc], G <: Geo](field: Field[Doc, List[G]],
                                                      from: Point,
                                                      sort: Boolean,
                                                      radius: Option[lightdb.distance.Distance]) extends Conversion[Doc, DistanceAndDoc[Doc]]

  /**
   * Yields each parent doc bundled with any join-related inner hits (typed child documents)
   * and per-hit highlight fragments. Only the OpenSearch backend implements inner_hits and
   * highlights natively — other backends return the wrapper with both fields empty.
   *
   * Inner-hits are configured per-relation via `Query.withInnerHits(...)`; highlights via
   * `Query.withHighlight(...)`. Both default off, so callers who don't opt in pay nothing.
   */
  case class DocWithInnerHits[Doc <: Document[Doc], Model <: DocumentModel[Doc]]() extends Conversion[Doc, lightdb.query.DocWithInnerHits[Doc, Model]]
}