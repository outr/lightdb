package lightdb.materialized

import fabric.Json
import lightdb.Field
import lightdb.doc.{Document, DocumentModel}

case class MaterializedIndex[Doc <: Document[Doc], Model <: DocumentModel[Doc]](json: Json, model: Model) extends Materialized[Doc, Model] {
  def get[V](f: Model => Field[Doc, V]): Option[V] = {
    val index = f(model)
    get(index.name, index.rw)
  }

  def apply[V](f: Model => Field[Doc, V]): V = {
    val index = f(model)
    apply(index.name, index.rw)
  }
}
