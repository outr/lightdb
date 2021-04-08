package lightdb

import cats.effect.IO
import cats.instances.list._
import cats.syntax.parallel._
import lightdb.store.ObjectStore

class LightDB(val store: ObjectStore) {
  private var _collections = List.empty[Collection[_]]

  def collection[T](mapping: ObjectMapping[T]): Collection[T] = synchronized {
    val c = Collection[T](this, mapping)
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