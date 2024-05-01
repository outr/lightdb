package lightdb.index

import cats.effect.IO
import lightdb.model.{AbstractCollection, Collection}
import lightdb.query.{PagedResults, Query, SearchContext}
import lightdb.Document

trait IndexSupport[D <: Document[D]] extends Collection[D] {
  lazy val query: Query[D] = Query(this)

  override def commit(): IO[Unit] = super.commit().flatMap(_ => index.commit())

  def index: Indexer[D]

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = index.withSearchContext(f)

  def doSearch(query: Query[D],
               context: SearchContext[D],
               offset: Int,
               after: Option[PagedResults[D]]): IO[PagedResults[D]]

  override def postSet(doc: D, collection: AbstractCollection[D]): IO[Unit] = for {
    _ <- indexDoc(doc, index.fields)
    _ <- super.postSet(doc, collection)
  } yield ()

  override def postDelete(doc: D, collection: AbstractCollection[D]): IO[Unit] = index.delete(doc._id).flatMap { _ =>
    super.postDelete(doc, collection)
  }

  protected def indexDoc(doc: D, fields: List[IndexedField[_, D]]): IO[Unit]
}