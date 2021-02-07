package lightdb.store

import lightdb.Id
import lightdb.util.ObjectTaskQueue

import scala.concurrent.{ExecutionContext, Future}

trait ObjectStore {
  def get[T](id: Id[T])(implicit ec: ExecutionContext): Future[Option[Array[Byte]]]

  def put[T](id: Id[T], value: Array[Byte])(implicit ec: ExecutionContext): Future[Array[Byte]]

  def delete[T](id: Id[T])(implicit ec: ExecutionContext): Future[Unit]

  def dispose()(implicit ec: ExecutionContext): Unit

  def modify[T](id: Id[T])(f: Option[Array[Byte]] => Option[Array[Byte]])(implicit ec: ExecutionContext): Future[Option[Array[Byte]]] = {
    ObjectTaskQueue(id.toString) {
      get[T](id).flatMap { current =>
        f(current) match {
          case None if current.isEmpty => Future.successful(None) // No value
          case None => delete[T](id).map(_ => None) // Delete existing value
          case Some(updated) => put[T](id, updated).map(array => Some(array)) // Set new value
        }
      }
    }
  }
}
