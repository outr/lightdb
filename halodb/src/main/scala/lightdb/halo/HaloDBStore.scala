package lightdb.halo

import lightdb._
import cats.effect.IO
import com.oath.halodb.{HaloDB, HaloDBOptions}
import lightdb.{Id, Store}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

case class HaloDBStore(directory: Path,
                       indexThreads: Int,
                       maxFileSize: Int) extends Store {
  private lazy val instance: HaloDB = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)
    opts.setMaxFileSize(maxFileSize)
    opts.setUseMemoryPool(true)
    opts.setMemoryPoolChunkSize(10 * 1024 * 1024)
    opts.setFlushDataSizeBytes(100 * 1024 * 1024)
    opts.setCompactionThresholdPerFile(0.9)
    opts.setCompactionJobRate(50 * 1024 * 1024)
    opts.setNumberOfRecords(100_000_000)
    opts.setCleanUpTombstonesDuringOpen(true)

    Files.createDirectories(directory.getParent)
    HaloDB.open(directory.toAbsolutePath.toString, opts)
  }

  override def keyStream[D]: fs2.Stream[IO, Id[D]] = fs2.Stream.fromBlockingIterator[IO](instance.newKeyIterator().asScala, 1024)
    .map { record =>
      Id[D](record.getBytes.string)
    }

  override def stream[D]: fs2.Stream[IO, (Id[D], Array[Byte])] = fs2.Stream.fromBlockingIterator[IO](instance.newIterator().asScala, 1024)
    .map { record =>
      val key = record.getKey.string
      Id[D](key) -> record.getValue
    }

  override def get[D](id: Id[D]): IO[Option[Array[Byte]]] = IO {
    Option(instance.get(id.bytes))
  }

  override def put[D](id: Id[D], value: Array[Byte]): IO[Boolean] = IO {
    instance.put(id.bytes, value)
  }

  override def delete[D](id: Id[D]): IO[Unit] = IO {
    instance.delete(id.bytes)
  }

  override def size: IO[Int] = IO(instance.size().toInt)

  override def commit(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = IO(instance.close())
}