package testdb
import scala.concurrent.{ExecutionContext, Future}

class MapStore(override implicit val executionContext: ExecutionContext) extends ObjectStore {
  private var map = Map.empty[Id[_], Array[Byte]]

  override def get[T](id: Id[T]): Future[Option[Array[Byte]]] = Future.successful(map.get(id))

  override def put[T](id: Id[T], value: Array[Byte]): Future[Array[Byte]] = synchronized {
    map += id -> value
    Future.successful(value)
  }

  override def delete[T](id: Id[T]): Future[Unit] = synchronized {
    map -= id
    Future.successful(())
  }

  override def dispose(): Unit = map = Map.empty
}
