package lightdb.aggregate

import cats.effect.IO
import lightdb.Document
import lightdb.index.Materialized
import lightdb.query.{Query, SearchContext}

case class AggregateQuery[D <: Document[D]](query: Query[D, _],
                                            functions: List[AggregateFunction[_, D]]) {
  def stream(implicit context: SearchContext[D]): fs2.Stream[IO, Materialized[D]] = query.indexSupport.aggregate(this)

  def toList: IO[List[Materialized[D]]] = query.indexSupport.withSearchContext { implicit context =>
    stream.compile.toList
  }
}
