package lightdb.sqlite

import cats.effect.IO
import lightdb.{Document, Id}
import lightdb.index.{IndexSupport, Indexer}
import lightdb.query.{PagedResults, Query, SearchContext}

trait SQLiteSupport[D <: Document[D]] extends IndexSupport[D] {
  override lazy val index: SQLiteIndexer[D] = SQLiteIndexer(this)

  override def doSearch(query: Query[D],
                        context: SearchContext[D],
                        offset: Int,
                        after: Option[PagedResults[D]]): IO[PagedResults[D]] = ???
}

case class SQLiteIndexer[D <: Document[D]](indexSupport: IndexSupport[D]) extends Indexer[D] {
  override def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = ???

  override def count(): IO[Int] = ???

  override private[lightdb] def delete(id: Id[D]): IO[Unit] = ???

  override def commit(): IO[Unit] = ???
}