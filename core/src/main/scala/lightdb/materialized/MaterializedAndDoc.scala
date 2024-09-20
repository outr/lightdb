package lightdb.materialized

import fabric.Json
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field

case class MaterializedAndDoc[Doc <: Document[Doc], Model <: DocumentModel[Doc]](json: Json, model: Model, doc: Doc) extends Materialized[Doc, Model] {
  def get[V](f: Model => Field[Doc, V]): Option[V] = {
    val index = f(model)
    get(index.name, index.rw)
  }

  def apply[V](f: Model => Field[Doc, V]): V = {
    val index = f(model)
    apply(index.name, index.rw)
  }

  def value(f: Model => Field[Doc, _]): Json = json(f(model).name)
}
