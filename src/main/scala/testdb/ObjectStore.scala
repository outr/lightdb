package testdb

import scala.concurrent.{ExecutionContext, Future}

trait ObjectStore {
  implicit def executionContext: ExecutionContext

  def get[T](id: Id[T]): Future[Option[Array[Byte]]]
  def put[T](id: Id[T], value: Array[Byte]): Future[Array[Byte]]
  def delete[T](id: Id[T]): Future[Unit]
  def dispose(): Unit

  def modify[T](id: Id[T])(f: Option[Array[Byte]] => Option[Array[Byte]]): Future[Option[Array[Byte]]] = {
    ObjectTaskQueue(id.toString) {
      get[T](id).flatMap { current =>
        f(current) match {
          case None if current.isEmpty => Future.successful(None)               // No value
          case None => delete[T](id).map(_ => None)                             // Delete existing value
          case Some(updated) => put[T](id, updated).map(array => Some(array))   // Set new value
        }
      }
    }
  }
}