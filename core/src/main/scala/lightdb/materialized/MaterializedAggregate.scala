package lightdb.materialized

import fabric.Json
import lightdb.aggregate.AggregateFunction
import lightdb.doc.{Document, DocumentModel}

case class MaterializedAggregate[Doc <: Document[Doc], Model <: DocumentModel[Doc]](json: Json, model: Model) extends Materialized[Doc, Model] {
  def get[T, V](f: Model => AggregateFunction[T, V, Doc]): Option[T] = {
    val function = f(model)
    get(function.name, function.tRW)
  }

  def apply[T, V](f: Model => AggregateFunction[T, V, Doc]): T = {
    val function = f(model)
    apply(function.name, function.tRW)
  }
}
