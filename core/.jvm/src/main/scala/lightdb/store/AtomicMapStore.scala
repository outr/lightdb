package lightdb.store

import cats.effect.IO
import fabric.rw.RW
import lightdb.{Id, LightDB}
import lightdb.document.Document
import lightdb.transaction.Transaction

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class AtomicMapStore[D <: Document[D]] extends Store[D] {
  override type Serialized = D

  private lazy val map = new ConcurrentHashMap[Id[D], D]

  override protected def initialize(): IO[Unit] = IO.unit

  override protected def serialize(doc: D): IO[D] = IO.pure(doc)

  override protected def deserialize(serialized: D): IO[D] = IO.pure(serialized)

  override protected def setSerialized(id: Id[D],
                                       serialized: D,
                                       transaction: Transaction[D]): IO[Boolean] = IO.blocking {
    map.put(id, serialized)
    true
  }

  override protected def getSerialized(id: Id[D],
                                       transaction: Transaction[D]): IO[Option[D]] = IO.blocking {
    Option(map.get(id))
  }

  override protected def streamSerialized(transaction: Transaction[D]): fs2.Stream[IO, D] = fs2.Stream
    .fromBlockingIterator[IO](map.values().iterator().asScala, 128)

  override def count(implicit transaction: Transaction[D]): IO[Int] = IO.blocking(map.size())

  override def idStream(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = fs2.Stream
    .fromBlockingIterator[IO](map.keys().asIterator().asScala, 128)

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): IO[Boolean] = IO.blocking {
    map.remove(id) != null
  }

  override def truncate()(implicit transaction: Transaction[D]): IO[Int] = IO.blocking {
    try {
      map.size()
    } finally {
      map.clear()
    }
  }

  override def dispose(): IO[Unit] = IO.blocking(map.clear())
}

object AtomicMapStore extends StoreManager {
  override protected def create[D <: Document[D]](db: LightDB, name: String)
                                                 (implicit rw: RW[D]): IO[Store[D]] = IO(new AtomicMapStore[D])
}