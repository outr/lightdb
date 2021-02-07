package lightdb.store

import lightdb.Id

import scala.concurrent.{ExecutionContext, Future}

class MapStore extends ObjectStore {
  private var map = Map.empty[Id[_], Array[Byte]]

  override def get[T](id: Id[T])(implicit ec: ExecutionContext): Future[Option[Array[Byte]]] = Future.successful(map.get(id))

  override def put[T](id: Id[T], value: Array[Byte])(implicit ec: ExecutionContext): Future[Array[Byte]] = synchronized {
    map += id -> value
    Future.successful(value)
  }

  override def delete[T](id: Id[T])(implicit ec: ExecutionContext): Future[Unit] = synchronized {
    map -= id
    Future.successful(())
  }

  override def dispose()(implicit ec: ExecutionContext): Unit = map = Map.empty
}
