package lightdb.aggregate

import cats.effect.IO
import lightdb.Document
import lightdb.index.Materialized
import lightdb.query.{Query, SearchContext}

case class AggregateQuery[D <: Document[D]](query: Query[D, _],
                                            functions: List[AggregateFunction[_, _, D]],
                                            filter: Option[AggregateFilter[D]] = None) {
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

  def stream(implicit context: SearchContext[D]): fs2.Stream[IO, Materialized[D]] = query.indexSupport.aggregate(this)

  def toList: IO[List[Materialized[D]]] = query.indexSupport.withSearchContext { implicit context =>
    stream.compile.toList
  }
}
