package lightdb.aggregate

import cats.effect.IO
import lightdb.document.Document
import lightdb.index.Materialized
import lightdb.query.{Query, SortDirection}
import lightdb.transaction.Transaction

case class AggregateQuery[D <: Document[D]](query: Query[D],
                                            functions: List[AggregateFunction[_, _, D]],
                                            filter: Option[AggregateFilter[D]] = None,
                                            sort: List[(AggregateFunction[_, _, D], SortDirection)] = Nil) {
  def filter(filter: AggregateFilter[D], and: Boolean = false): AggregateQuery[D] = {
    if (and && this.filter.nonEmpty) {
      copy(filter = Some(this.filter.get && filter))
    } else {
      copy(filter = Some(filter))
    }
  }

  def filters(filters: AggregateFilter[D]*): AggregateQuery[D] = if (filters.nonEmpty) {
    var filter = filters.head
    filters.tail.foreach { f =>
      filter = filter && f
    }
    this.filter(filter)
  } else {
    this
  }

  def sort(function: AggregateFunction[_, _, D],
           direction: SortDirection = SortDirection.Ascending): AggregateQuery[D] = copy(
    sort = sort ::: List((function, direction))
  )

  def stream(implicit transaction: Transaction[D]): fs2.Stream[IO, Materialized[D]] = query.indexer.aggregate(this)

  def toList(implicit transaction: Transaction[D]): IO[List[Materialized[D]]] = stream.compile.toList
}
