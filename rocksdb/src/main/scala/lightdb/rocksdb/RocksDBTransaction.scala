package lightdb.rocksdb

import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.{DuplicateIdException, StoreDisposedException}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.rocksdb.RocksDBTransaction.{UndoPrior, writeOptions}
import lightdb.store.Store
import lightdb.store.write.WriteOp
import lightdb.transaction.{PrefixScanningTransaction, Transaction, UndoableTransaction}
import org.rocksdb.{ReadOptions, RocksIterator, Slice, Snapshot, WriteBatch, WriteOptions}
import profig.Profig
import rapid.*

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

case class RocksDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
  store: RocksDBStore[Doc, Model],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]
) extends PrefixScanningTransaction[Doc, Model] with UndoableTransaction[Doc, Model] { tx =>
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

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
    if index == store.idField then {
      val bytes = value.asInstanceOf[Id[Doc]].bytes
      Option(store.native(store.handle match {
        case Some(h) => store.rocksDB.get(h, bytes)
        case None => store.rocksDB.get(bytes)
      })).map(bytes2Doc)
    } else {
      throw new UnsupportedOperationException(s"RocksDBStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override def getAll(ids: Seq[Id[Doc]]): rapid.Stream[Doc] = rapid.Stream.force(Task {
    val keyBytes = ids.map(_.bytes).asJava

    rapid.Stream.fromIterator(Task {
      val rawResults = store.native(store.handle match {
        case Some(h) =>
          val handleList = java.util.Collections.nCopies(ids.size, h)
          store.rocksDB.multiGetAsList(handleList, keyBytes)
        case None =>
          store.rocksDB.multiGetAsList(keyBytes)
      })
      rawResults
        .asScala
        .iterator
        .filter(_ != null)
        .map(bytes2Doc)
    })
  })

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    val base = beforeWrite(Iterator.single(doc._id))
    val json = doc.json(store.model.rw)
    val bytes = JsonFormatter.Compact(json).getBytes(StandardCharsets.UTF_8)
    store.native(store.handle match {
      case Some(h) => store.rocksDB.put(h, writeOptions, doc._id.bytes, bytes)
      case None => store.rocksDB.put(writeOptions, doc._id.bytes, bytes)
    })
    afterWrite(base, 1)
    doc
  }

  override def upsert(stream: rapid.Stream[Doc], commitEvery: Option[Int] = None): Task[Int] = commitEvery match {
    case Some(_) => super.upsert(stream, commitEvery)
    case None => Task.defer {
      batchUpsert(stream.evalTap { doc =>
        store.trigger.upsert(doc, this)
      })
    }
  }

  private def batchUpsert(stream: rapid.Stream[Doc]): Task[Int] = Task.defer {
    // Chunk to bound batch size and memory usage.
    val chunkSize = math.max(1, Profig("lightdb.rocksdb.batch.chunkSize").opt[Int].getOrElse(Store.MaxInsertBatch))
    stream
      .chunk(chunkSize)
      .fold(0)((total, chunk) => Task(store.native {
        val batch = new WriteBatch
        try {
          val base = beforeWrite(chunk.iterator.map(_._id))
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
          afterWrite(base, chunk.length)
          streamBatchedRows.addAndGet(chunk.length)
          total + chunk.length
        } finally {
          batch.close()
        }
      }))
  }

  def flushOps(ops: Seq[WriteOp[Doc]]): Task[Unit] = Task(store.native {
    // Honor the insert contract: every WriteOp.Insert must error if the id already exists.
    // RocksDB's underlying `put` is upsert-style, so we pre-check existence via `keyExists`
    // before adding inserts to the batch. multiGet would be cheaper for very large batches
    // but adds a value-materialization cost — keyExists is bloom-filter-fast and fits the
    // common case (small inserts batched alongside upserts/deletes).
    val inserts = ops.iterator.collect { case WriteOp.Insert(d) => d }
    inserts.foreach { doc =>
      val exists = store.handle match {
        case Some(h) => store.rocksDB.keyExists(h, doc._id.bytes)
        case None => store.rocksDB.keyExists(doc._id.bytes)
      }
      if (exists) throw DuplicateIdException(store.name, doc._id)
      remember(doc._id)
    }
    val base = beforeWrite(ops.iterator.collect {
      case WriteOp.Upsert(doc) => doc._id
      case WriteOp.Delete(id) => id
    })

    val batch = new WriteBatch
    try {
      ops.foreach {
        case WriteOp.Insert(doc) =>
          val json = doc.json(store.model.rw)
          val bytes = JsonFormatter.Compact(json).getBytes(StandardCharsets.UTF_8)
          store.handle match {
            case Some(h) => batch.put(h, doc._id.bytes, bytes)
            case None => batch.put(doc._id.bytes, bytes)
          }
        case WriteOp.Upsert(doc) =>
          val json = doc.json(store.model.rw)
          val bytes = JsonFormatter.Compact(json).getBytes(StandardCharsets.UTF_8)
          store.handle match {
            case Some(h) => batch.put(h, doc._id.bytes, bytes)
            case None => batch.put(doc._id.bytes, bytes)
          }
        case WriteOp.Delete(id) =>
          store.handle match {
            case Some(h) => batch.delete(h, id.bytes)
            case None => batch.delete(id.bytes)
          }
      }
      store.rocksDB.write(writeOptions, batch)
      afterWrite(base, ops.size)
      batchedRows.addAndGet(ops.size)
    } finally {
      batch.close()
    }
  })

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task {
    store.native(store.handle match {
      case Some(h) => store.rocksDB.keyExists(h, id.bytes)
      case None => store.rocksDB.keyExists(id.bytes)
    })
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
    store.native(store.rocksDB.getLongProperty(store.handle.orNull, "rocksdb.estimate-num-keys")).toInt
  }

  override protected def _delete(id: Id[Doc]): Task[Boolean] = Task {
    val base = beforeWrite(Iterator.single(id))
    val bytes = id.bytes
    store.native(store.handle match {
      case Some(h) => store.rocksDB.delete(h, writeOptions, bytes)
      case None => store.rocksDB.delete(writeOptions, bytes)
    })
    afterWrite(base, 1)
    true
  }

  override protected def _commit: Task[Unit] = Task(undoLock.synchronized {
    clearUndo()
    releaseSnapshots()
  })

  override protected def _rollback: Task[Unit] = Task(undoWrites())

  override protected def _close: Task[Unit] = Task(undoLock.synchronized {
    clearUndo()
    releaseSnapshots()
  })

  // -- Undo ----------------------------------------------------------------------------------------------------------
  // Once `recordUndo` is called, the state each id had before this transaction first wrote it, so a rollback can put
  // storage back: a write-through store has no other way to take a written row back. A write does not read the rows it
  // replaces: the log names a RocksDB snapshot that still shows them, so they stay in the database, pinned by the
  // snapshot, and only a rollback reads them. One snapshot serves the transaction's writes for as long as nothing else
  // has written to the database since it was taken (the sequence numbers since are all this transaction's, each on an
  // id the log already names); a write after another writer's takes a new one. An insert records its id as absent. A
  // truncate that drops the column family reads the pending rows first, since the drop takes them.
  @volatile private var undo: ConcurrentHashMap[String, UndoPrior] = null
  // Guarded by undoLock, as is every read through a snapshot, so none is released while in use.
  private val undoSnapshots = new java.util.ArrayList[Snapshot]()
  private var current: UndoPrior.At = null
  // The latest sequence number `current` covers: every write after its own sequence number and up to this one is this
  // transaction's. -1 when a write may have come from elsewhere.
  private var currentThrough: Long = -1L
  private val undoLock = new Object

  override def recordUndo(): Unit = undoLock.synchronized {
    if undo == null then undo = new ConcurrentHashMap[String, UndoPrior]()
  }

  private def remember(id: Id[Doc]): Unit = {
    val log = undo
    if log != null then log.putIfAbsent(id.value, UndoPrior.Held(None))
  }

  // Called just before a write: names a snapshot that still shows the prior row of each id the transaction writes first,
  // and returns the sequence number the write follows, for afterWrite (-1 when not recording undo).
  private def beforeWrite(ids: Iterator[Id[Doc]]): Long = {
    val log = undo
    if log == null then -1L
    else undoLock.synchronized {
      val latest = store.native(store.rocksDB.getLatestSequenceNumber)
      val first = ids.filterNot(id => log.containsKey(id.value))
      if !first.hasNext then latest
      else {
        val base = if current != null && latest == currentThrough then latest
        else {
          val snapshot = store.native(store.rocksDB.getSnapshot())
          if undoSnapshots.isEmpty then store.undoing.add(this)
          undoSnapshots.add(snapshot)
          current = UndoPrior.At(snapshot)
          currentThrough = snapshot.getSequenceNumber
          currentThrough
        }
        first.foreach(id => log.putIfAbsent(id.value, current))
        base
      }
    }
  }

  // Called after a write of `ops` operations that followed sequence number `base`: the current snapshot keeps serving
  // only if the write took exactly the next `ops` sequence numbers, so no other writer came between.
  private def afterWrite(base: Long, ops: Int): Unit = if base >= 0L then undoLock.synchronized {
    if current != null && base == currentThrough then {
      val latest = store.native(store.rocksDB.getLatestSequenceNumber)
      currentThrough = if latest == base + ops then latest else -1L
    }
  }

  // The prior rows of the ids whose entries name a snapshot, read through it. Called under undoLock.
  private def readPending(log: ConcurrentHashMap[String, UndoPrior]): Map[String, Option[Array[Byte]]] =
    log.asScala.toList.collect { case (id, UndoPrior.At(snapshot)) => snapshot -> id }.groupMap(_._1)(_._2).flatMap {
      case (snapshot, ids) =>
        val readOptions = new ReadOptions().setSnapshot(snapshot)
        try {
          val keys = ids.map(_.getBytes(StandardCharsets.UTF_8)).asJava
          val values = store.native(store.handle match {
            case Some(h) => store.rocksDB.multiGetAsList(readOptions, java.util.Collections.nCopies(ids.size, h), keys)
            case None => store.rocksDB.multiGetAsList(readOptions, keys)
          })
          undoReads.addAndGet(ids.size)
          ids.iterator.zip(values.asScala.iterator.map(Option(_)))
        } finally readOptions.close()
    }

  /** Reads the rows this transaction's undo log names through snapshots and holds them instead, releasing the
    * snapshots: called before a truncate drops the column family the snapshots would be read from. */
  private[rocksdb] def holdPriors(): Unit = undoLock.synchronized {
    val log = undo
    if log != null then readPending(log).foreach((id, before) => log.put(id, UndoPrior.Held(before)))
    releaseSnapshots()
  }

  private def releaseSnapshots(): Unit = undoLock.synchronized {
    current = null
    currentThrough = -1L
    if !undoSnapshots.isEmpty then {
      store.undoing.remove(this)
      store.guard.ifOpen(undoSnapshots.forEach(s => store.rocksDB.releaseSnapshot(s)))
      undoSnapshots.clear()
    }
  }

  // Counters a spec reads: prior rows read for the undo log, rows written through a WriteBatch by flushOps and by the
  // stream upsert, the prior row values the undo log holds in memory, and the snapshots it holds.
  private[rocksdb] val undoReads = new java.util.concurrent.atomic.AtomicLong(0L)
  private[rocksdb] val batchedRows = new java.util.concurrent.atomic.AtomicLong(0L)
  private[rocksdb] val streamBatchedRows = new java.util.concurrent.atomic.AtomicLong(0L)

  private[rocksdb] def undoSnapshotsHeld: Int = undoLock.synchronized(undoSnapshots.size)

  private[rocksdb] def undoRowsHeld: Int = {
    val log = undo
    if log == null then 0 else log.values().asScala.count {
      case UndoPrior.Held(Some(_)) => true
      case _ => false
    }
  }

  private def clearUndo(): Unit = {
    val log = undo
    if log != null then log.clear()
  }

  private def undoWrites(): Unit = undoLock.synchronized {
    val log = undo
    try {
      if log != null && !log.isEmpty then store.native {
        val pending = readPending(log)
        val batch = new WriteBatch
        try {
          log.forEach { (id, entry) =>
            val key = id.getBytes(StandardCharsets.UTF_8)
            val before = entry match {
              case UndoPrior.Held(bytes) => bytes
              case UndoPrior.At(_) => pending(id)
            }
            (before, store.handle) match {
              case (Some(bytes), Some(h)) => batch.put(h, key, bytes)
              case (Some(bytes), None) => batch.put(key, bytes)
              case (None, Some(h)) => batch.delete(h, key)
              case (None, None) => batch.delete(key)
            }
          }
          store.rocksDB.write(writeOptions, batch)
        } finally batch.close()
      }
    } finally {
      clearUndo()
      releaseSnapshots()
    }
  }

  override def truncate: Task[Int] = Task.defer {
    // Rows this or another open transaction must be able to put back are read before the truncate removes them.
    store.undoing.synchronized(store.undoing.asScala.toList).foreach(_.holdPriors())
    // Fast path: drop the entire column family and recreate it. O(1) in the
    // store size — vs `truncateManual` which iterates every key. For stores
    // with millions of entries (e.g. `entityRelationshipTraversal` after a
    // dedup run) this is the difference between minutes and milliseconds.
    //
    // Requires the handle-cache eviction in `RocksDBSharedStore` so the
    // recreate sees a fresh CF instead of resurrecting the dead handle.
    //
    // For the row-count return value, use RocksDB's `rocksdb.estimate-num-keys`
    // property — an O(1) approximation built from SST metadata. We
    // intentionally do NOT call `count`, which iterates every key and would
    // negate the entire point of the fast path. Truncate's return is
    // informational; an estimate is sufficient.
    val handleOpt = store.handle
    val sharedOpt = store.sharedStore
    if (handleOpt.isDefined && sharedOpt.isDefined) {
      val h = handleOpt.get
      val ss = sharedOpt.get
      Task(store.native {
        val estimate = try {
          val raw = store.rocksDB.getProperty(h, "rocksdb.estimate-num-keys")
          if (raw == null || raw.isEmpty) 0
          else raw.toLong.min(Int.MaxValue.toLong).toInt
        } catch { case _: Throwable => 0 }
        store.rocksDB.dropColumnFamily(h)
        ss.store.evictHandle(ss.handle)
        store.resetHandle()
        estimate
      })
    } else {
      truncateManual
    }
  }

  /** Per-key delete fallback used when the column-family fast path can't be
   * taken (no shared store / no handle). Deletes are accumulated into bounded
   * `WriteBatch` chunks and flushed periodically rather than into a single
   * unbounded batch — that prior shape caused tens-of-GB heap + native-buffer
   * peaks on large stores (and a real-world OOM-kill on a `~tens-of-millions`
   * key truncate).
   *
   * Chunk size is conservative: 50k keys → roughly low-MB per batch even
   * with long keys. Tunable via `lightdb.rocksdb.truncate.batchSize` profig
   * if a workload needs different bounds.
   */
  private def truncateManual: Task[Int] = Task(store.native {
    val batchSize = profig.Profig("lightdb.rocksdb.truncate.batchSize")
      .opt[Int].filter(_ > 0).getOrElse(50_000)
    val iter = store.handle match {
      case Some(h) => store.rocksDB.newIterator(h)
      case None => store.rocksDB.newIterator()
    }
    var batch = new WriteBatch()
    var count = 0
    var inBatch = 0

    val delete = store.handle match {
      case Some(h) => (key: Array[Byte]) => batch.delete(h, key)
      case None => (key: Array[Byte]) => batch.delete(key)
    }

    def flush(): Unit = {
      if (inBatch > 0) {
        store.rocksDB.write(writeOptions, batch)
        batch.close()
        batch = new WriteBatch()
        inBatch = 0
      }
    }

    try {
      iter.seekToFirst()
      while iter.isValid do {
        delete(iter.key())
        count += 1
        inBatch += 1
        if (inBatch >= batchSize) flush()
        iter.next()
      }
      flush()
    } finally {
      try iter.close() catch { case _: Throwable => () }
      try batch.close() catch { case _: Throwable => () }
    }
    count
  })

  private def iterator(value: Boolean = true,
                       prefix: Option[String] = None): Iterator[Array[Byte]] with AutoCloseable = {
    if store.isDisposing then throw StoreDisposedException(store.name)
    val lease = try store.iteratorThreadPool.acquire() catch {
      case _: IllegalStateException => throw StoreDisposedException(store.name)
    }
    lazy val iterator: ThreadConfinedBufferedIterator[Array[Byte]] = new ThreadConfinedBufferedIterator[Array[Byte]](
      mk = new Iterator[Array[Byte]] with AutoCloseable {
      private def prefixUpperBoundBytes(p: Array[Byte]): Option[Array[Byte]] = {
        // Compute the shortest byte array that is lexicographically greater than any key starting with p.
        // Example: "abc" => "abd" (as bytes). If p is all 0xFF, there is no upper bound.
        var i = p.length - 1
        while i >= 0 && (p(i) & 0xff) == 0xff do i -= 1
        if i < 0 then None
        else {
          val out = java.util.Arrays.copyOf(p, i + 1)
          out(i) = (out(i) + 1).toByte
          Some(out)
        }
      }

      private def startsWithBytes(key: Array[Byte], p: Array[Byte]): Boolean = {
        if key.length < p.length then false
        else {
          var i = 0
          var matches = true
          while i < p.length && matches do {
            if key(i) != p(i) then matches = false
            i += 1
          }
          matches
        }
      }

      private lazy val readOptions: ReadOptions = {
        val ro = new ReadOptions()
        // For prefix scans, tell RocksDB we only care about keys sharing the seek prefix.
        // This can reduce internal work during iteration.
        if prefix.nonEmpty then ro.setPrefixSameAsStart(true)
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
        if !hasNext then throw new NoSuchElementException("No more elements in the RocksDB iterator")

        val result = if value then {
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
        store.iterators.unregister(iterator)
        store.iteratorThreadPool.release(lease)
      },
      guard = Some(store.guard),
      storeName = store.name
    )
    if !store.iterators.register(iterator) then {
      store.iteratorThreadPool.release(lease)
      throw StoreDisposedException(store.name)
    }
    iterator
  }
}

object RocksDBTransaction {
  /** The state an id had before a transaction first wrote it: held (None: absent), or readable through a snapshot. */
  private[rocksdb] enum UndoPrior {
    case Held(bytes: Option[Array[Byte]])
    case At(snapshot: Snapshot)
  }

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
    if disableWALForBulkWrites then bulkWriteOptionsNoWAL else bulkWriteOptionsWAL
}