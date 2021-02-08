package lightdb.collection

import cats.effect.IO
import lightdb.data.DataManager
import lightdb.{Id, LightDB}

trait Collection[T] {
  protected def db: LightDB
  protected def dataManager: DataManager[T]

  def get(id: Id[T]): IO[Option[T]] = db.store.get(id).map(_.map(dataManager.fromArray))
  def put(id: Id[T], value: T): IO[T] = db.store.put(id, dataManager.toArray(value)).map(_ => value)
  def modify(id: Id[T])(f: Option[T] => Option[T]): IO[Option[T]] = {
    var result: Option[T] = None
    db.store.modify(id) { bytes =>
      val value = bytes.map(dataManager.fromArray)
      result = f(value)
      result.map(dataManager.toArray)
    }.map(_ => result)
  }
  def delete(id: Id[T]): IO[Unit] = db.store.delete(id)
}