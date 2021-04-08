package lightdb

import cats.effect.IO
import cats.instances.list._
import cats.syntax.parallel._
import lightdb.collection.Collection
import lightdb.index.Indexer
import lightdb.store.ObjectStore

import java.nio.file.Path

// TODO: extract ObjectStore to each collection?
abstract class LightDB(val directory: Option[Path], val store: ObjectStore) {
  private var _collections = List.empty[Collection[_]]

  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D]

  def collection[D <: Document[D]](name: String, mapping: ObjectMapping[D]): Collection[D] = synchronized {
    val c = Collection[D](this, mapping, name)
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