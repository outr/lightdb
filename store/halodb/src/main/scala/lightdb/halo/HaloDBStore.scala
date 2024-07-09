package lightdb.halo

import com.oath.halodb.{HaloDB, HaloDBOptions}
import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import lightdb.{Id, LightDB}
import lightdb.document.{Document, SetType}
import lightdb.store.{Store, StoreManager}
import lightdb.transaction.Transaction

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import lightdb.store._

case class HaloDBStore[D <: Document[D]](directory: Path,
                          indexThreads: Int = Runtime.getRuntime.availableProcessors(),
                          maxFileSize: Int = 1024 * 1024,
                          internalCounter: Boolean = false)(implicit val rw: RW[D]) extends ByteStore[D] {
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

  override def idIterator(implicit transaction: Transaction[D]): Iterator[Id[D]] = instance.newKeyIterator().asScala
    .map { record =>
      Id[D](record.getBytes.string)
    }

  override def iterator(implicit transaction: Transaction[D]): Iterator[D] = instance.newIterator().asScala
    .map(_.getValue).map(bytes2D)

  override def get(id: Id[D])(implicit transaction: Transaction[D]): Option[D] = Option(instance.get(id.bytes)).map(bytes2D)

  override def put(id: Id[D], doc: D)(implicit transaction: Transaction[D]): Option[SetType] = {
    val `type` = if (!internalCounter) {
      SetType.Unknown
    } else if (contains(id)) {
      SetType.Replace
    } else {
      SetType.Insert
    }
    if (instance.put(id.bytes, d2Bytes(doc))) {
      Some(`type`)
    } else {
      None
    }
  }

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): Boolean = {
    val exists = contains(id)
    instance.delete(id.bytes)
    exists
  }

  override def count(implicit transaction: Transaction[D]): Int = instance.size().toInt

  override def commit()(implicit transaction: Transaction[D]): Unit = ()

  override def dispose(): Unit = {
    instance.pauseCompaction()
    instance.close()
  }
}

object HaloDBStore extends StoreManager {
  override protected def create[D <: Document[D]](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new HaloDBStore(db.directory.get.resolve(name))
}