package lightdb.index

import fabric.Json
import lightdb.aggregate.AggregateFunction
import lightdb.document.{Document, DocumentModel}

case class MaterializedAggregate[D <: Document[D], M <: DocumentModel[D]](json: Json, model: M) extends Materialized[D] {
  def get[T, F](f: M => AggregateFunction[T, F, D]): Option[T] = {
    val function = f(model)
    get(function.name, function.tRW)
  }

  def apply[T, F](f: M => AggregateFunction[T, F, D]): T = {
    val function = f(model)
    apply(function.name, function.tRW)
  }
}
