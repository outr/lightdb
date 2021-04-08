package lightdb.store

import cats.effect.IO
import lightdb.Id

class MapStore extends ObjectStore {
  private var map = Map.empty[Id[_], Array[Byte]]

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

  override def flush(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = {
    map = Map.empty
    IO.unit
  }
}
