package lightdb.store.halo

import cats.effect.IO
import cats.effect.kernel._
import fs2.Stream
import com.oath.halodb.{HaloDB, HaloDBOptions}
import lightdb.store.ObjectStore
import lightdb.Id

import java.nio.file.Path
import scala.jdk.CollectionConverters._

case class HaloStore(directory: Path, indexThreads: Int = 2) extends ObjectStore {
  private val halo = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)

    HaloDB.open(directory.toAbsolutePath.toString, opts)
  }

  override def all[T](chunkSize: Int = 512)(implicit F: Sync[IO]): Stream[IO, (Id[T], Array[Byte])] = Stream
    .fromBlockingIterator
    .apply(halo.newIterator().asScala, chunkSize)
    .map(r => Id[T](new String(r.getKey, "UTF-8")) -> r.getValue)

  override def get[T](id: Id[T]): IO[Option[Array[Byte]]] = IO {
    halo.newIterator()
    Option(halo.get(id.bytes))
  }

  override def put[T](id: Id[T], value: Array[Byte]): IO[Array[Byte]] = IO {
    halo.put(id.bytes, value)
  }.map(_ => value)

  override def delete[T](id: Id[T]): IO[Unit] = IO {
    halo.delete(id.bytes)
  }

  override def count(): IO[Long] = IO {
    halo.size()
  }

  override def commit(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = IO(halo.close())
}