package lightdb.rocksdb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{StoreManager, StoreMode}
import lightdb.util.Disposable
import org.rocksdb.{ColumnFamilyHandle, RocksDB}
import rapid.Task

import java.nio.charset.StandardCharsets
import java.nio.file.Path

case class RocksDBSharedStore(directory: Path) extends StoreManager with Disposable {
  RocksDB.loadLibrary()

  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = RocksDBStore[Doc, Model]

  val (rocksDB: RocksDB, existingHandles: List[ColumnFamilyHandle]) = RocksDBStore.createRocksDB(directory)

  // Mutable so the fast-truncate path (drop column family + recreate) can
  // evict the dropped handle. Otherwise `RocksDBStore.resetHandle()` would
  // re-attach the dead handle from this map and every subsequent op would
  // fail. `TrieMap` is thread-safe; truncate is rare but reads happen often.
  private val handlesMap: scala.collection.concurrent.TrieMap[String, ColumnFamilyHandle] =
    scala.collection.concurrent.TrieMap.from(existingHandles.map { h =>
      new String(h.getName, StandardCharsets.UTF_8) -> h
    })

  def existingHandlesMap: scala.collection.Map[String, ColumnFamilyHandle] = handlesMap

  /** Remove a handle from the cache (called by the fast-truncate path after
   * `dropColumnFamily`). Subsequent `resetHandle()` calls will create a fresh
   * column family of the same name. */
  private[rocksdb] def evictHandle(name: String): Unit = { handlesMap.remove(name); () }

  /** Register a freshly-created handle so other stores looking up the same
   * shared name see it. */
  private[rocksdb] def registerHandle(name: String, handle: ColumnFamilyHandle): Unit = {
    handlesMap.update(name, handle); ()
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new RocksDBStore[Doc, Model](
      name = name,
      path = path,
      model = model,
      rocksDB = rocksDB,
      sharedStore = Some(RocksDBSharedStoreInstance(this, name)),
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )

  /**
   * Close the underlying native RocksDB instance.
   *
   * Wired into the standard dispose chain via `Disposable` so `LightDB.doDispose` reaches it
   * (per-store `doDispose`s only flush their own column family handle and do NOT close the
   * shared RocksDB). Without this, native compaction/WAL/flush threads stay alive past test
   * exit and try to call back into Java during JVM shutdown — at which point the system
   * classloader is closed and any unloaded class (e.g. `FlushOptions`) throws
   * `NoClassDefFoundError`.
   */
  override protected def doDispose(): Task[Unit] = Task(rocksDB.closeE())
}