package lightdb.index

import cats.effect.IO
import lightdb.query.Query
import lightdb.{Collection, Document}

trait IndexSupport[D <: Document[D], IF[F] <: IndexedField[F, D]] extends Collection[D] {
  lazy val query: Query[D] = Query(this)

  protected def indexer: Indexer[D]

  override def commit(): IO[Unit] = super.commit().flatMap(_ => indexer.commit())

  def index: IndexManager[D, IF]
}

trait IndexManager[D <: Document[D], IF <: IndexedField[_, D]] {
  protected var _fields = List.empty[IF]

  def fields: List[IF] = _fields

  protected[lightdb] def register[F, Field <: IF with IndexedField[F, D]](field: Field): Unit = synchronized {
    _fields = field :: _fields
  }
}