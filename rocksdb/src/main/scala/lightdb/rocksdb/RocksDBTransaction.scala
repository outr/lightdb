package lightdb.rocksdb

import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.rocksdb.RocksDBTransaction.writeOptions
import lightdb.store.Store
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import org.rocksdb.{ReadOptions, RocksIterator, Slice, WriteBatch, WriteOptions}
import rapid._

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

case class RocksDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: RocksDBStore[Doc, Model],
                                                                                 parent: Option[Transaction[Doc, Model]]) extends PrefixScanningTransaction[Doc, Model] { tx =>
  private def bytes2Doc(bytes: Array[Byte]): Doc = bytes2Json(bytes).as[Doc](store.model.rw)

  private def bytes2Json(bytes: Array[Byte]): Json = {
    val jsonString = new String(bytes, StandardCharsets.UTF_8)
    JsonParser(jsonString)
  }

  override def jsonPrefixStream(prefix: String): rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(iterator(prefix = Some(prefix)))).map(bytes2Json)

  /**
   * Key-only prefix scan. This avoids JSON parsing and document decoding and is much cheaper when the caller
   * only needs to count/inspect ids to detect fanout.
   */
  def keyPrefixStream(prefix: String): rapid.Stream[String] = rapid.Stream
    .fromIterator(Task(iterator(value = false, prefix = Some(prefix))))
    .map(bytes => new String(bytes, StandardCharsets.UTF_8))

  override def jsonStream: rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(iterator())).map(bytes2Json)

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    if (index == store.idField) {
      val bytes = value.asInstanceOf[Id[Doc]].bytes
      Option(store.handle match {
        case Some(h) => store.rocksDB.get(h, bytes)
        case None => store.rocksDB.get(bytes)
      }).map(bytes2Doc)
    } else {
      throw new UnsupportedOperationException(s"RocksDBStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override def getAll(ids: Seq[Id[Doc]]): rapid.Stream[Doc] = rapid.Stream.force(Task {
    val keyBytes = ids.map(_.bytes).asJava

    rapid.Stream.fromIterator(Task {
      val rawResults = store.handle match {
        case Some(h) =>
          val handleList = java.util.Collections.nCopies(ids.size, h)
          store.rocksDB.multiGetAsList(handleList, keyBytes)
        case None =>
          store.rocksDB.multiGetAsList(keyBytes)
      }
      rawResults
        .asScala
        .iterator
        .filter(_ != null)
        .map(bytes2Doc)
    })
  })

  override protected def _insert(doc: Doc): Task[Doc] = _upsert(doc)

  override def insert(stream: rapid.Stream[Doc]): Task[Int] = Task.defer {
    batchUpsert(stream.evalTap { doc =>
      store.trigger.insert(doc, this)
    })
  }

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    val json = doc.json(store.model.rw)
    val bytes = JsonFormatter.Compact(json).getBytes(StandardCharsets.UTF_8)
    store.handle match {
      case Some(h) => store.rocksDB.put(h, writeOptions, doc._id.bytes, bytes)
      case None => store.rocksDB.put(writeOptions, doc._id.bytes, bytes)
    }
    doc
  }

  override def upsert(stream: rapid.Stream[Doc]): Task[Int] = Task.defer {
    batchUpsert(stream.evalTap { doc =>
      store.trigger.upsert(doc, this)
    })
  }

  private def batchUpsert(stream: rapid.Stream[Doc]): Task[Int] = Task.defer {
    // Chunk to bound batch size and memory usage.
    val chunkSize = math.max(1, Store.MaxInsertBatch)
    stream
      .chunk(chunkSize)
      .fold(0)((total, chunk) => Task {
        val batch = new WriteBatch
        try {
          chunk.foreach { doc =>
            val json = doc.json(store.model.rw)
            val bytes = JsonFormatter.Compact(json).getBytes(StandardCharsets.UTF_8)
            store.handle match {
              case Some(h) => batch.put(h, doc._id.bytes, bytes)
              case None => batch.put(doc._id.bytes, bytes)
            }
          }
          // Bulk writes can optionally skip WAL for faster rebuild/regeneration jobs.
          store.rocksDB.write(RocksDBTransaction.bulkWriteOptions, batch)
          total + chunk.length
        } finally {
          batch.close()
        }
      })
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task {
    store.handle match {
      case Some(h) => store.rocksDB.keyExists(h, id.bytes)
      case None => store.rocksDB.keyExists(id.bytes)
    }
  }

  override protected def _count: Task[Int] = Task {
    val i = iterator(value = false)
    try {
      i.size
    } finally {
      i.close()
    }
  }

  override def estimatedCount: Task[Int] = Task {
    store.rocksDB.getLongProperty(store.handle.orNull, "rocksdb.estimate-num-keys").toInt
  }

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] = Task {
    val bytes = value.asInstanceOf[Id[Doc]].bytes
    store.handle match {
      case Some(h) => store.rocksDB.delete(h, writeOptions, bytes)
      case None => store.rocksDB.delete(writeOptions, bytes)
    }
    true
  }

  override protected def _commit: Task[Unit] = Task.unit

  override protected def _rollback: Task[Unit] = Task.unit

  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = Task.defer {
    truncateManual
    // TODO: Revisit column family dropping - it's causing issues right now
    /*store.handle match {
      case Some(h) => count.map { size =>
        store.rocksDB.dropColumnFamily(h)
        store.resetHandle()
        size
      }
      case None => truncateManual
    }*/
  }

  private def truncateManual: Task[Int] = Task {
    val iter = store.handle match {
      case Some(h) => store.rocksDB.newIterator(h)
      case None => store.rocksDB.newIterator()
    }
    val batch = new WriteBatch()
    var count = 0

    val delete = store.handle match {
      case Some(h) => (key: Array[Byte]) => batch.delete(h, key)
      case None => (key: Array[Byte]) => batch.delete(key)
    }

    iter.seekToFirst()
    while (iter.isValid) {
      delete(iter.key())
      count += 1
      iter.next()
    }
    store.rocksDB.write(writeOptions, batch)
    count
  }

  private def iterator(value: Boolean = true,
                       prefix: Option[String] = None): Iterator[Array[Byte]] with AutoCloseable = {
    val lease = store.iteratorThreadPool.acquire()
    new ThreadConfinedBufferedIterator[Array[Byte]](
      mk = new Iterator[Array[Byte]] with AutoCloseable {
      private def prefixUpperBoundBytes(p: Array[Byte]): Option[Array[Byte]] = {
        // Compute the shortest byte array that is lexicographically greater than any key starting with p.
        // Example: "abc" => "abd" (as bytes). If p is all 0xFF, there is no upper bound.
        var i = p.length - 1
        while (i >= 0 && (p(i) & 0xff) == 0xff) i -= 1
        if (i < 0) None
        else {
          val out = java.util.Arrays.copyOf(p, i + 1)
          out(i) = (out(i) + 1).toByte
          Some(out)
        }
      }

      private def startsWithBytes(key: Array[Byte], p: Array[Byte]): Boolean = {
        if (key.length < p.length) return false
        var i = 0
        while (i < p.length) {
          if (key(i) != p(i)) return false
          i += 1
        }
        true
      }

      private lazy val readOptions: ReadOptions = {
        val ro = new ReadOptions()
        // For prefix scans, tell RocksDB we only care about keys sharing the seek prefix.
        // This can reduce internal work during iteration.
        if (prefix.nonEmpty) ro.setPrefixSameAsStart(true)
        ro
      }
      private lazy val prefixBytes: Option[Array[Byte]] = prefix.map(_.getBytes(StandardCharsets.UTF_8))
      private lazy val upperBoundSlice: Option[Slice] =
        prefixBytes.flatMap(prefixUpperBoundBytes).map(new Slice(_))
      private lazy val rocksIterator = {
        // If we can compute an exclusive upper bound for this prefix, let RocksDB enforce it.
        // This avoids per-entry prefix checking (and allocations) in our iterator loop.
        upperBoundSlice.foreach(readOptions.setIterateUpperBound)
        val i = store.handle match {
          case Some(h) => store.rocksDB.newIterator(h, readOptions)
          case None => store.rocksDB.newIterator(readOptions)
        }
        i
      }

      prefix match {
        case Some(s) => rocksIterator.seek(s.getBytes(StandardCharsets.UTF_8))     // Initialize the iterator to the prefix
        case None => rocksIterator.seekToFirst()                    // Seek to the provided value as the start position
      }

      val isValid: () => Boolean = prefixBytes match {
        case Some(pBytes) if upperBoundSlice.isEmpty =>
          // No upper bound available (pathological), so fall back to a cheap, allocation-free prefix check.
          () => rocksIterator.isValid && startsWithBytes(rocksIterator.key(), pBytes)
        case None =>
          () => rocksIterator.isValid
        case Some(_) =>
          // Upper bound is enforced by RocksDB; no need for manual prefix checks.
          () => rocksIterator.isValid
      }

      override def hasNext: Boolean = isValid()

      override def next(): Array[Byte] = {
        if (!hasNext) throw new NoSuchElementException("No more elements in the RocksDB iterator")

        val result = if (value) {
          rocksIterator.value()
        } else {
          rocksIterator.key()
        }

        // Move to the next entry after retrieving the current value or key
        rocksIterator.next()
        result
      }

      override def close(): Unit = {
        try {
          rocksIterator.close()
        } finally {
          upperBoundSlice.foreach(_.close())
          readOptions.close()
        }
      }
    },
      name = s"rocksdb-iterator-${store.name}",
      batchSize = 1024,
      sharedAgent = Some(lease),
      onClose = () => {
        store.iteratorThreadPool.release(lease)
      }
    )
  }
}

object RocksDBTransaction {
  /**
   * Default write options (WAL enabled). This is used for single-document writes and deletes.
   */
  val writeOptions: WriteOptions = new WriteOptions()
    .setSync(false)
    .setDisableWAL(false)

  private val bulkWriteOptionsWAL: WriteOptions = new WriteOptions()
    .setSync(false)
    .setDisableWAL(false)

  private val bulkWriteOptionsNoWAL: WriteOptions = new WriteOptions()
    .setSync(false)
    .setDisableWAL(true)

  /**
   * When true, bulk batched writes (stream insert/upsert) will disable WAL for higher throughput.
   * Defaults to false for safety.
   */
  @volatile var disableWALForBulkWrites: Boolean = false

  def bulkWriteOptions: WriteOptions =
    if (disableWALForBulkWrites) bulkWriteOptionsNoWAL else bulkWriteOptionsWAL
}