package lightdb.store

import lightdb.Field
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedIndex
import lightdb.spatial.{DistanceAndDoc, Geo}

sealed trait Conversion[Doc, V]

object Conversion {
  case class Value[Doc <: Document[Doc], F](field: Field[Doc, F]) extends Conversion[Doc, F]

  case class Doc[Doc <: Document[Doc]]() extends Conversion[Doc, Doc]

  case class Json[Doc <: Document[Doc]](fields: List[Field[Doc, _]]) extends Conversion[Doc, fabric.Json]

  case class Materialized[Doc <: Document[Doc], Model <: DocumentModel[Doc]](fields: List[Field[Doc, _]]) extends Conversion[Doc, MaterializedIndex[Doc, Model]]

  case class Converted[Doc <: Document[Doc], T](f: Doc => T) extends Conversion[Doc, T]

  case class Distance[Doc <: Document[Doc]](field: Field[Doc, Option[Geo.Point]],
                           from: Geo.Point,
                           sort: Boolean,
                           radius: Option[lightdb.distance.Distance]) extends Conversion[Doc, DistanceAndDoc[Doc]]
}