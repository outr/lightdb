package lightdb.materialized

import fabric.Json
import lightdb.aggregate.AggregateFunction
import lightdb.doc.{Document, DocumentModel}

case class MaterializedAggregate[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
                                                                                     json: Json,
                                                                                     model: Model,
                                                                                     /**
                                                                                      * Per-outer-bucket inner aggregation results, keyed by the sub-aggregate
                                                                                      * function's [[AggregateFunction.name]]. Each value is the list of inner
                                                                                      * buckets (or a single-element list for non-Group sub-aggregates) computed
                                                                                      * within this outer bucket. Empty when the outer function had no
                                                                                      * sub-aggregates declared, or when the backend can't compute sub-aggs.
                                                                                      */
                                                                                     subResults: Map[String, List[MaterializedAggregate[Doc, Model]]] = Map.empty[String, List[MaterializedAggregate[Doc, Model]]]
                                                                                   ) extends Materialized[Doc, Model] {
  def get[T, V](f: Model => AggregateFunction[T, V, Doc]): Option[T] = {
    val function = f(model)
    get(function.name, function.tRW)
  }

  def apply[T, V](f: Model => AggregateFunction[T, V, Doc]): T = {
    val function = f(model)
    apply(function.name, function.tRW)
  }

  /** Inner buckets produced by the named sub-aggregation, or `Nil` if absent. */
  def subResultsOf(name: String): List[MaterializedAggregate[Doc, Model]] =
    subResults.getOrElse(name, Nil)

  /** Inner buckets produced by the given sub-aggregation function, or `Nil` if absent. */
  def subResultsOf(f: Model => AggregateFunction[_, _, Doc]): List[MaterializedAggregate[Doc, Model]] =
    subResultsOf(f(model).name)
}
