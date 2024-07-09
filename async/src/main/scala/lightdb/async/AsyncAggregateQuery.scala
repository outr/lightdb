package lightdb.async

import cats.effect.IO
import lightdb.aggregate.{AggregateFilter, AggregateFunction, AggregateQuery}
import lightdb.doc.DocModel
import lightdb.materialized.MaterializedAggregate
import lightdb.{Query, SortDirection, Transaction}

case class AsyncAggregateQuery[Doc, Model <: DocModel[Doc]](query: Query[Doc, Model],
                                                            functions: List[AggregateFunction[_, _, Doc]],
                                                            filter: Option[AggregateFilter[Doc]] = None,
                                                            sort: List[(AggregateFunction[_, _, Doc], SortDirection)] = Nil) {
  def filter(filter: AggregateFilter[Doc], and: Boolean = false): AsyncAggregateQuery[Doc, Model] = {
    if (and && this.filter.nonEmpty) {
      copy(filter = Some(this.filter.get && filter))
    } else {
      copy(filter = Some(filter))
    }
  }

  def filters(filters: AggregateFilter[Doc]*): AsyncAggregateQuery[Doc, Model] = if (filters.nonEmpty) {
    var filter = filters.head
    filters.tail.foreach { f =>
      filter = filter && f
    }
    this.filter(filter)
  } else {
    this
  }

  def sort(function: AggregateFunction[_, _, Doc],
           direction: SortDirection = SortDirection.Ascending): AsyncAggregateQuery[Doc, Model] = copy(
    sort = sort ::: List((function, direction))
  )

  private lazy val aggregateQuery = AggregateQuery(
    query = query,
    functions = functions,
    filter = filter,
    sort = sort
  )

  def count(implicit transaction: Transaction[Doc]): IO[Int] =
    IO.blocking(query.collection.store.aggregateCount(aggregateQuery))

  def stream(implicit transaction: Transaction[Doc]): fs2.Stream[IO, MaterializedAggregate[Doc, Model]] = {
    val iterator = query.collection.store.aggregate(aggregateQuery)
    fs2.Stream.fromBlockingIterator[IO](iterator, 100)
  }

  def toList(implicit transaction: Transaction[Doc]): IO[List[MaterializedAggregate[Doc, Model]]] = stream.compile.toList
}
