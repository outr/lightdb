package lightdb.sqlite

import cats.effect.IO
import lightdb.{Document, Id}
import lightdb.index.{IndexSupport, Indexer}
import lightdb.query.{PagedResults, Query, SearchContext}

import java.nio.file.{Files, Path}
import java.sql.{Connection, DriverManager}

trait SQLiteSupport[D <: Document[D]] extends IndexSupport[D] {
  private lazy val path: Path = db.directory.resolve("sqlite.db")
  // TODO: Should each collection have a connection?
  private lazy val connection: Connection = {
    val c = DriverManager.getConnection(s"jdbc:sqlite:${path.toFile.getCanonicalPath}")
    c.setAutoCommit(false)
    c
  }

  override lazy val index: SQLiteIndexer[D] = SQLiteIndexer(this)

  override def doSearch(query: Query[D],
                        context: SearchContext[D],
                        offset: Int,
                        after: Option[PagedResults[D]]): IO[PagedResults[D]] = ???

  override def dispose(): IO[Unit] = super.dispose().map { _ =>
    connection.close()
  }
}

case class SQLiteIndexer[D <: Document[D]](indexSupport: SQLiteSupport[D]) extends Indexer[D] {
  override def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return] = ???

  override def count(): IO[Int] = ???

  override private[lightdb] def delete(id: Id[D]): IO[Unit] = ???

  override def commit(): IO[Unit] = ???


}