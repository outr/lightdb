package lightdb.collection

import lightdb.data.DataManager
import lightdb.{Id, LightDB}

import scala.concurrent.{ExecutionContext, Future}

trait Collection[T] {
  protected def db: LightDB
  protected def dataManager: DataManager[T]

  def get(id: Id[T])(implicit ec: ExecutionContext): Future[Option[T]] = db.store.get(id).map(_.map(dataManager.fromArray))
  def put(id: Id[T], value: T)(implicit ec: ExecutionContext): Future[T] = db.store.put(id, dataManager.toArray(value)).map(_ => value)
  def modify(id: Id[T])(f: Option[T] => Option[T])(implicit ec: ExecutionContext): Future[Option[T]] = {
    var result: Option[T] = None
    db.store.modify(id) { bytes =>
      val value = bytes.map(dataManager.fromArray)
      result = f(value)
      result.map(dataManager.toArray)
    }.map(_ => result)
  }
  def delete(id: Id[T])(implicit ec: ExecutionContext): Future[Unit] = db.store.delete(id)
}