package lightdb.store

import cats.effect.IO
import fabric.rw.RW
import lightdb.{Id, LightDB}
import lightdb.document.Document
import lightdb.transaction.Transaction

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class AtomicMapStore extends Store {
  private lazy val map = new ConcurrentHashMap[String, Array[Byte]]

  override def keyStream[D]: fs2.Stream[IO, Id[D]] = fs2.Stream
    .fromBlockingIterator[IO](map.keys().asIterator().asScala.map(Id.apply[D]), 128)

  override def stream: fs2.Stream[IO, Array[Byte]] = fs2.Stream
    .fromBlockingIterator[IO](map.values().iterator().asScala, 128)

  override def get[D](id: Id[D]): IO[Option[Array[Byte]]] = IO.blocking {
    Option(map.get(id.value))
  }

  override def put[D](id: Id[D], value: Array[Byte]): IO[Boolean] = IO.blocking {
    map.put(id.value, value)
    true
  }

  override def delete[D](id: Id[D]): IO[Unit] = IO.blocking {
    map.remove(id.value)
  }

  override def count: IO[Int] = IO.blocking(map.size())

  override def commit(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = IO.blocking(map.clear())
}

object AtomicMapStore extends StoreManager {
  override protected def create(db: LightDB, name: String): Store = new AtomicMapStore
}