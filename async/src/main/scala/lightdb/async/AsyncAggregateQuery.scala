package lightdb.async

import cats.effect.IO
import lightdb.aggregate.{AggregateFilter, AggregateFunction, AggregateQuery}
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.transaction.Transaction
import lightdb.{Query, SortDirection}

case class AsyncAggregateQuery[Doc <: Document[Doc], Model <: DocumentModel[Doc]](query: Query[Doc, Model],
                                                                 functions: List[AggregateFunction[_, _, Doc]],
                                                                 filter: Option[AggregateFilter[Doc]] = None,
                                                                 sort: List[(AggregateFunction[_, _, Doc], SortDirection)] = Nil) {
  def filter(f: Model => AggregateFilter[Doc], and: Boolean = false): AsyncAggregateQuery[Doc, Model] = {
    val filter = f(query.model)
    if (and && this.filter.nonEmpty) {
      copy(filter = Some(this.filter.get && filter))
    } else {
      copy(filter = Some(filter))
    }
  }

  def filters(f: Model => List[AggregateFilter[Doc]]): AsyncAggregateQuery[Doc, Model] = {
    val filters = f(query.model)
    if (filters.nonEmpty) {
      var filter = filters.head
      filters.tail.foreach { f =>
        filter = filter && f
      }
      this.filter(_ => filter)
    } else {
      this
    }
  }

  def sort(f: Model => AggregateFunction[_, _, Doc],
           direction: SortDirection = SortDirection.Ascending): AsyncAggregateQuery[Doc, Model] = copy(
    sort = sort ::: List((f(query.model), direction))
  )

  private lazy val aggregateQuery = AggregateQuery(
    query = query,
    functions = functions,
    filter = filter,
    sort = sort
  )

  def count(implicit transaction: Transaction[Doc]): IO[Int] =
    IO.blocking(query.store.aggregateCount(aggregateQuery))

  def stream(implicit transaction: Transaction[Doc]): fs2.Stream[IO, MaterializedAggregate[Doc, Model]] = {
    val iterator = query.store.aggregate(aggregateQuery)
    fs2.Stream.fromBlockingIterator[IO](iterator, 100)
  }

  def toList(implicit transaction: Transaction[Doc]): IO[List[MaterializedAggregate[Doc, Model]]] = stream.compile.toList
}
