package lightdb.index

import cats.effect.IO
import lightdb.query.{PagedResults, Query, SearchContext}
import lightdb.{Collection, Document}

trait IndexSupport[D <: Document[D]] extends Collection[D] {
  lazy val query: Query[D] = Query(this)

  override def commit(): IO[Unit] = super.commit().flatMap(_ => index.commit())

  def index: Indexer[D]

  def doSearch(query: Query[D],
               context: SearchContext[D],
               offset: Int,
               after: Option[PagedResults[D]]): IO[PagedResults[D]]
}