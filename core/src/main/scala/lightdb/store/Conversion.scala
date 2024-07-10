package lightdb.store

import lightdb.Field
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedIndex
import lightdb.spatial.{DistanceAndDoc, GeoPoint}

sealed trait Conversion[Doc, V]

object Conversion {
  case class Value[Doc, F](field: Field[Doc, F]) extends Conversion[Doc, F]

  case class Doc[Doc]() extends Conversion[Doc, Doc]

  case class Json[Doc](fields: List[Field[Doc, _]]) extends Conversion[Doc, fabric.Json]

  case class Materialized[Doc <: Document[Doc], Model <: DocumentModel[Doc]](fields: List[Field[Doc, _]]) extends Conversion[Doc, MaterializedIndex[Doc, Model]]

  case class Converted[Doc, T](f: Doc => T) extends Conversion[Doc, T]

  case class Distance[Doc](field: Field[Doc, GeoPoint],
                           from: GeoPoint,
                           sort: Boolean,
                           radius: Option[lightdb.distance.Distance]) extends Conversion[Doc, DistanceAndDoc[Doc]]
}