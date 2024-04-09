package lightdb.index

import cats.effect.IO
import lightdb.query.{PagedResults, Query, SearchContext}
import lightdb.{Collection, Document}

trait IndexSupport[D <: Document[D]] extends Collection[D] {
  lazy val query: Query[D] = Query(this)

  protected def indexer: Indexer[D]

  override def commit(): IO[Unit] = super.commit().flatMap(_ => indexer.commit())

  def index: IndexManager[D]

  def doSearch(query: Query[D],
               context: SearchContext[D],
               offset: Int,
               after: Option[PagedResults[D]]): IO[PagedResults[D]]
}