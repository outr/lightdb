package lightdb.store

import cats.effect.IO
import cats.effect.kernel.Sync
import lightdb.Id

class MapStore extends ObjectStore {
  private var map = Map.empty[Id[_], Array[Byte]]

  override def all[T](chunkSize: Int = 512): fs2.Stream[IO, ObjectData[T]] = fs2.Stream
    .fromBlockingIterator[IO](map.iterator, chunkSize)
    .map {
      case (id, data) => ObjectData(id.asInstanceOf[Id[T]], data)
    }

  override def get[T](id: Id[T]): IO[Option[Array[Byte]]] = IO.pure(map.get(id))

  override def put[T](id: Id[T], value: Array[Byte]): IO[Array[Byte]] = synchronized {
    map += id -> value
    IO.pure(value)
  }

  override def delete[T](id: Id[T]): IO[Unit] = synchronized {
    map -= id
    IO.unit
  }

  override def count(): IO[Long] = IO.pure(map.size)

  override def commit(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = {
    map = Map.empty
    IO.unit
  }

  override def truncate(): IO[Unit] = synchronized {
    map = Map.empty
    IO.unit
  }
}
