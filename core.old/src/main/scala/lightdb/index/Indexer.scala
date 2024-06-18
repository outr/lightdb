package lightdb.index

import cats.effect.IO
import fabric.rw.RW
import lightdb.model.Collection
import lightdb.query.SearchContext
import lightdb.{Document, Id, query}

trait Indexer[D <: Document[D]] {
  protected var _fields = List.empty[Index[_, D]]

  def fields: List[Index[_, D]] = _fields

  def indexSupport: IndexSupport[D]

  def truncate(): IO[Unit]

  def commit(): IO[Unit]

  def size: IO[Int]

  def withSearchContext[Return](f: SearchContext[D] => IO[Return]): IO[Return]

  protected[lightdb] def register[F](field: Index[F, D]): Unit = synchronized {
    fields.find(_.fieldName == field.fieldName) match {
      case Some(existing) if existing != field => throw new RuntimeException(s"Index already exists: ${field.fieldName}")
      case Some(_) => // Don't add again
      case None => _fields = field :: _fields
    }
  }

  private[lightdb] def delete(id: Id[D]): IO[Unit]
}