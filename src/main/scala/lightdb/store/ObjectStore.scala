package lightdb.store

import cats.effect.IO
import lightdb.Id
import lightdb.util.ObjectLock

trait ObjectStore {
  def get[T](id: Id[T]): IO[Option[Array[Byte]]]

  def put[T](id: Id[T], value: Array[Byte]): IO[Array[Byte]]

  def delete[T](id: Id[T]): IO[Unit]

  def dispose(): IO[Unit]

  def modify[T](id: Id[T])(f: Option[Array[Byte]] => Option[Array[Byte]]): IO[Option[Array[Byte]]] = {
    ObjectLock.io(id.toString) {
      get[T](id).flatMap { current =>
        f(current) match {
          case None if current.isEmpty => IO.pure(None) // No value
          case None => delete[T](id).map(_ => None) // Delete existing value
          case Some(updated) => put[T](id, updated).map(array => Some(array)) // Set new value
        }
      }
    }
  }

  def count(): IO[Long]
}
