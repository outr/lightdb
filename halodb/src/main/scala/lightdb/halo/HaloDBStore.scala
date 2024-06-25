package lightdb.halo

import cats.effect.IO
import com.oath.halodb.{HaloDB, HaloDBOptions}
import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.RW
import lightdb.{Id, LightDB}
import lightdb.document.Document
import lightdb.store.{Store, StoreManager}
import lightdb.transaction.Transaction

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

import lightdb.store._

case class HaloDBStore(directory: Path,
                       indexThreads: Int = Runtime.getRuntime.availableProcessors(),
                       maxFileSize: Int = 1024 * 1024) extends Store {
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

  override def stream: fs2.Stream[IO, Array[Byte]] = fs2.Stream.fromBlockingIterator[IO](instance.newIterator().asScala, 1024)
    .map(_.getValue)

  override def get[D](id: Id[D]): IO[Option[Array[Byte]]] = IO.blocking {
    Option(instance.get(id.bytes))
  }

  override def put[D](id: Id[D], value: Array[Byte]): IO[Boolean] = IO.blocking {
    instance.put(id.bytes, value)
  }

  override def delete[D](id: Id[D]): IO[Unit] = IO.blocking {
    instance.delete(id.bytes)
  }

  override def count: IO[Int] = IO.blocking(instance.size().toInt)

  override def commit(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = IO.blocking {
    instance.pauseCompaction()
    instance.close()
  }
}

object HaloDBStore extends StoreManager {
  override protected def create(db: LightDB, name: String): Store = new HaloDBStore(db.directory.resolve(name))
}