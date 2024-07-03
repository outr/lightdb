package lightdb.materialized

import fabric.Json
import lightdb.Field
import lightdb.doc.DocModel

case class MaterializedIndex[Doc, Model <: DocModel[Doc]](json: Json, model: Model) extends Materialized[Doc, Model] {
  def get[V](f: Model => Field[Doc, V]): Option[V] = {
    val index = f(model)
    get(index.name, index.rw)
  }

  def apply[V](f: Model => Field[Doc, V]): V = {
    val index = f(model)
    apply(index.name, index.rw)
  }
}
