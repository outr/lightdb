package lightdb.halo

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

case class HaloDBStore[D](directory: Path,
                       indexThreads: Int = Runtime.getRuntime.availableProcessors(),
                       maxFileSize: Int = 1024 * 1024)(implicit val rw: RW[D]) extends ByteStore[D] {
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

  override def idIterator: Iterator[Id[D]] = instance.newKeyIterator().asScala
    .map { record =>
      Id[D](record.getBytes.string)
    }

  override def iterator: Iterator[D] = instance.newIterator().asScala
    .map(_.getValue).map(bytes2D)

  override def get(id: Id[D]): Option[D] = Option(instance.get(id.bytes)).map(bytes2D)

  override def put(id: Id[D], doc: D): Boolean = instance.put(id.bytes, d2Bytes(doc))

  override def delete(id: Id[D]): Unit = instance.delete(id.bytes)

  override def count: Int = instance.size().toInt

  override def commit(): Unit = ()

  override def dispose(): Unit = {
    instance.pauseCompaction()
    instance.close()
  }
}

object HaloDBStore extends StoreManager {
  override protected def create[D](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new HaloDBStore(db.directory.resolve(name))
}