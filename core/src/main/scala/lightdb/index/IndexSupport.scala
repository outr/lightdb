package lightdb.index

import cats.effect.IO
import lightdb.query.{PagedResults, Query, SearchContext}
import lightdb.{Collection, Document}

trait IndexSupport[D <: Document[D]] extends Collection[D] {
  lazy val query: Query[D] = Query(this)

  override def commit(): IO[Unit] = super.commit().flatMap(_ => index.commit())

  def index: Indexer[D]

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = index.withSearchContext(f)

  def doSearch(query: Query[D],
               context: SearchContext[D],
               offset: Int,
               after: Option[PagedResults[D]]): IO[PagedResults[D]]

  override protected def postSet(doc: D): IO[Unit] = for {
    _ <- indexDoc(doc, index.fields)
    _ <- super.postSet(doc)
  } yield ()

  override protected def postDelete(doc: D): IO[Unit] = index.delete(doc._id).flatMap { _ =>
    super.postDelete(doc)
  }

  protected def indexDoc(doc: D, fields: List[IndexedField[_, D]]): IO[Unit]
}