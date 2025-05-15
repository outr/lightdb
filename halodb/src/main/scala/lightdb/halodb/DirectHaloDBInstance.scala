package lightdb.halodb

import com.oath.halodb.{HaloDB, HaloDBOptions}
import fabric.io.{JsonFormatter, JsonParser}
import fabric.{Json, Null}
import lightdb.id.Id
import rapid.Task

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.language.implicitConversions

class DirectHaloDBInstance(directory: Path,
                           indexThreads: Int = Runtime.getRuntime.availableProcessors(),
                           maxFileSize: Int = 1024 * 1024 * 1024,
                           useMemoryPool: Boolean = true,
                           memoryPoolChunkSize: Int = 16 * 1024 * 1024,
                           flushDataSizeBytes: Int = 128 * 1024 * 1024,
                           compactionThresholdPerFile: Double = 0.9,
                           compactionJobRate: Int = 64 * 1024 * 1024,
                           numberOfRecords: Int = 100_000_000,
                           cleanUpTombstonesDuringOpen: Boolean = true) extends HaloDBInstance {
  val counter = new AtomicInteger(0)

  val db: HaloDB = create()

  private implicit def json2Bytes(json: Json): Array[Byte] = JsonFormatter.Compact(json).getBytes(StandardCharsets.UTF_8)

  private implicit def bytes2Json(bytes: Array[Byte]): Json = {
    if (bytes == null) {
      Null
    } else {
      val jsonString = new String(bytes, StandardCharsets.UTF_8)
      JsonParser(jsonString)
    }
  }

  override def put[Doc](id: Id[Doc], json: Json): Task[Unit] = Task(db.put(id.bytes, json))

  override def get[Doc](id: Id[Doc]): Task[Option[Json]] = Task {
    val result = db.get(id.bytes)
    if (result != null) {
      Option(result)
    } else {
      None
    }
  }

  override def exists[Doc](id: Id[Doc]): Task[Boolean] = Task(db.get(id.bytes) != null)

  override def count: Task[Int] = Task(db.size().toInt)

  override def stream: rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(db.newIterator().asScala.map(_.getValue)))

  override def delete[Doc](id: Id[Doc]): Task[Unit] = Task(db.delete(id.bytes))

  override def truncate(): Task[Int] = Task {
    val size = db.size().toInt
    if (size == 0) {
      0
    } else {
      db.newIterator().asScala.foreach(r => db.delete(r.getKey))
      size + truncate().sync()
    }
  }

  override def dispose(): Task[Unit] = Task {
    db.pauseCompaction()
    db.close()
  }

  protected def create(): HaloDB = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)
    opts.setMaxFileSize(maxFileSize)
    opts.setUseMemoryPool(useMemoryPool)
    opts.setMemoryPoolChunkSize(memoryPoolChunkSize)
    opts.setFlushDataSizeBytes(flushDataSizeBytes)
    opts.setCompactionThresholdPerFile(compactionThresholdPerFile)
    opts.setCompactionJobRate(compactionJobRate)
    opts.setNumberOfRecords(numberOfRecords)
    opts.setCleanUpTombstonesDuringOpen(cleanUpTombstonesDuringOpen)

    Files.createDirectories(directory.getParent)
    HaloDB.open(directory.toAbsolutePath.toString, opts)
  }
}
