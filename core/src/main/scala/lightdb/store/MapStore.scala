package lightdb.store

import cats.effect.IO
import lightdb.Id
import lightdb.document.Document
import lightdb.transaction.Transaction

/**
 * Simple in-memory Store backed by Map.
 *
 * Note: It is recommended to use AtomicMapStore on the JVM as a more efficient alternative to this.
 */
class MapStore[D <: Document[D]] extends Store[D] { store =>
  override type Serialized = D

  private var map = Map.empty[Id[D], D]

  override protected def initialize(): IO[Unit] = IO.unit

  override protected def serialize(doc: D): IO[D] = IO.pure(doc)

  override protected def deserialize(serialized: D): IO[D] = IO.pure(serialized)

  override protected def setSerialized(id: Id[D],
                                       serialized: D,
                                       transaction: Transaction[D]): IO[Boolean] = IO.blocking {
    store.synchronized {
      map += id -> serialized
    }
    true
  }

  override protected def getSerialized(id: Id[D],
                                       transaction: Transaction[D]): IO[Option[D]] = IO.blocking {
    map.get(id)
  }

  override protected def streamSerialized(transaction: Transaction[D]): fs2.Stream[IO, D] = fs2.Stream
    .fromBlockingIterator[IO](map.values.iterator, 128)

  override def count(implicit transaction: Transaction[D]): IO[Int] = IO.blocking(map.size)

  override def idStream(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = fs2.Stream
    .fromBlockingIterator[IO](map.keys.iterator, 128)

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): IO[Boolean] = IO.blocking {
    store.synchronized {
      if (map.contains(id)) {
        map -= id
        true
      } else {
        false
      }
    }
  }

  override def truncate()(implicit transaction: Transaction[D]): IO[Int] = IO.blocking {
    try {
      map.size
    } finally {
      store.synchronized {
        map = Map.empty
      }
    }
  }

  override def dispose(): IO[Unit] = IO.blocking(store.synchronized {
    map = Map.empty
  })
}

object MapStore extends StoreManager {
  override protected def create[D <: Document[D]](name: String): IO[Store[D]] = IO(new MapStore[D])
}