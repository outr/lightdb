package lightdb.aggregate

import cats.effect.IO
import lightdb.document.{Document, DocumentModel}
import lightdb.index.{Index, Materialized, MaterializedAggregate}
import lightdb.query.{Query, SortDirection}
import lightdb.transaction.Transaction

case class AggregateQuery[D <: Document[D], M <: DocumentModel[D]](query: Query[D, M],
                                            functions: List[AggregateFunction[_, _, D]],
                                            filter: Option[AggregateFilter[D]] = None,
                                            sort: List[(AggregateFunction[_, _, D], SortDirection)] = Nil) {
  def filter(filter: AggregateFilter[D], and: Boolean = false): AggregateQuery[D, M] = {
    if (and && this.filter.nonEmpty) {
      copy(filter = Some(this.filter.get && filter))
    } else {
      copy(filter = Some(filter))
    }
  }

  def filters(filters: AggregateFilter[D]*): AggregateQuery[D, M] = if (filters.nonEmpty) {
    var filter = filters.head
    filters.tail.foreach { f =>
      filter = filter && f
    }
    this.filter(filter)
  } else {
    this
  }

  def sort(function: AggregateFunction[_, _, D],
           direction: SortDirection = SortDirection.Ascending): AggregateQuery[D, M] = copy(
    sort = sort ::: List((function, direction))
  )

  def stream(implicit transaction: Transaction[D]): fs2.Stream[IO, MaterializedAggregate[D, M]] = query.indexer.aggregate(this)

  def toList(implicit transaction: Transaction[D]): IO[List[MaterializedAggregate[D, M]]] = stream.compile.toList
}
