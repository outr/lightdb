package lightdb.index

import cats.effect.IO
import lightdb.query.SearchContext
import lightdb.{Collection, Document, Id, query}

trait Indexer[D <: Document[D]] {
  protected var _fields = List.empty[IndexedField[_, D]]

  def fields: List[IndexedField[_, D]] = _fields

  def indexSupport: IndexSupport[D]

  def commit(): IO[Unit]

  def count(): IO[Int]

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return]

  protected[lightdb] def register[F](field: IndexedField[F, D]): Unit = synchronized {
    _fields = field :: _fields
  }

  private[lightdb] def delete(id: Id[D]): IO[Unit]
}