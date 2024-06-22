package lightdb.halo

import cats.effect.IO
import com.oath.halodb.{HaloDB, HaloDBOptions}
import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.RW
import lightdb.{Id, LightDB}
import lightdb.document.Document
import lightdb.store.{JsonStore, Store, StoreManager}
import lightdb.transaction.Transaction

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class HaloDBStore[D <: Document[D]](directory: Path,
                                    indexThreads: Int,
                                    maxFileSize: Int)
                                   (implicit val rw: RW[D]) extends JsonStore[D] {
  override type Serialized = Array[Byte]

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

  override protected def initialize(): IO[Unit] = IO(instance)

  override protected def json2Serialized(json: Json): IO[Array[Byte]] =
    IO(JsonFormatter.Compact(json).getBytes("UTF-8"))

  override protected def serialized2Json(serialized: Array[Byte]): IO[Json] = IO {
    val jsonString = new String(serialized, "UTF-8")
    JsonParser(jsonString)
  }

  override protected def setSerialized(id: Id[D],
                                       serialized: Array[Byte],
                                       transaction: Transaction[D]): IO[Boolean] = IO.blocking {
    instance.put(id.bytes, serialized)
  }

  override protected def getSerialized(id: Id[D], transaction: Transaction[D]): IO[Option[Array[Byte]]] = IO.blocking {
    Option(instance.get(id.bytes))
  }

  override protected def streamSerialized(transaction: Transaction[D]): fs2.Stream[IO, Array[Byte]] = fs2.Stream
    .fromBlockingIterator[IO](instance.newIterator().asScala, 1024)
    .map(_.getValue)

  override def count(implicit transaction: Transaction[D]): IO[Int] = IO.blocking(instance.size().toInt)

  override def idStream(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = fs2.Stream
    .fromBlockingIterator[IO](instance.newKeyIterator().asScala, 1024)
    .map(k => Id[D](new String(k.getBytes, "UTF-8")))

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): IO[Boolean] = IO.blocking {
    instance.delete(id.bytes)
    true
  }

  override def truncate()(implicit transaction: Transaction[D]): IO[Int] =
    idStream.evalMap(delete).compile.count.map(_.toInt)

  override def dispose(): IO[Unit] = IO.blocking {
    instance.pauseCompaction()
    instance.close()
  }
}

object HaloDBStore extends StoreManager {
  override protected def create[D <: Document[D]](db: LightDB, name: String)(implicit rw: RW[D]): IO[Store[D]] = IO {
    new HaloDBStore[D](db.directory.resolve(name), 32, 1024 * 1024)
  }
}