package lightdb.store

import cats.effect.IO
import fabric.rw.RW
import lightdb.{Id, LightDB}
import lightdb.document.Document
import lightdb.transaction.Transaction

/**
 * Simple in-memory Store backed by Map.
 *
 * Note: It is recommended to use AtomicMapStore on the JVM as a more efficient alternative to this.
 */
class MapStore extends Store { store =>
  private var map = Map.empty[String, Array[Byte]]

  override def keyStream[D]: fs2.Stream[IO, Id[D]] = fs2.Stream
    .fromBlockingIterator[IO](map.keys.iterator.map(Id.apply[D]), 128)

  override def stream: fs2.Stream[IO, Array[Byte]] = fs2.Stream
    .fromBlockingIterator[IO](map.values.iterator, 128)

  override def get[D](id: Id[D]): IO[Option[Array[Byte]]] = IO.blocking {
    map.get(id.value)
  }

  override def put[D](id: Id[D], value: Array[Byte]): IO[Boolean] = IO.blocking {
    store.synchronized {
      map += id.value -> value
    }
    true
  }

  override def delete[D](id: Id[D]): IO[Unit] = IO.blocking {
    store.synchronized {
      map -= id.value
    }
  }

  override def count: IO[Int] = IO.blocking(map.size)

  override def commit(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = IO.blocking(store.synchronized {
    map = Map.empty
  })
}

object MapStore extends StoreManager {
  override protected def create(db: LightDB, name: String): Store = new MapStore
}