package lightdb.index

import cats.effect.IO
import lightdb.Document

trait IndexManager[D <: Document[D]] {
  protected var _fields = List.empty[IndexedField[_, D]]

  def fields: List[IndexedField[_, D]] = _fields

  def count(): IO[Int]

  protected[lightdb] def register[F](field: IndexedField[F, D]): Unit = synchronized {
    _fields = field :: _fields
  }
}
