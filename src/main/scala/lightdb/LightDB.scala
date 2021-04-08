package lightdb

import cats.effect.IO
import cats.instances.list._
import cats.syntax.parallel._
import lightdb.collection.Collection
import lightdb.index.Indexer
import lightdb.store.ObjectStore

class LightDB(val store: ObjectStore, val indexer: Indexer) {
  private var _collections = List.empty[Collection[_]]

  def collection[D <: Document[D]](mapping: ObjectMapping[D]): Collection[D] = synchronized {
    val c = Collection[D](this, mapping)
    _collections = _collections ::: List(c)
    c
  }

  def dispose(): IO[Unit] = for {
    _ <- _collections.map(_.dispose()).parSequence
    _ <- store.dispose()
  } yield {
    ()
  }
}