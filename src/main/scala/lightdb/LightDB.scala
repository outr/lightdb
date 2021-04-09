package lightdb

import cats.effect.IO
import cats.instances.list._
import cats.syntax.parallel._
import lightdb.collection.Collection
import lightdb.index.Indexer
import lightdb.store.ObjectStore

import java.nio.file.Path

abstract class LightDB(val directory: Option[Path]) {
  private var _collections = List.empty[Collection[_]]

  def store[D <: Document[D]](collection: Collection[D]): ObjectStore
  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D]

  def collection[D <: Document[D]](name: String, mapping: ObjectMapping[D]): Collection[D] = synchronized {
    val c = Collection[D](this, mapping, name)
    _collections = _collections ::: List(c)
    c
  }

  def dispose(): IO[Unit] = _collections.map(_.dispose()).parSequence.map(_ => ())
}