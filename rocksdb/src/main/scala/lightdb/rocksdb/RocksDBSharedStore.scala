package lightdb.rocksdb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{StoreManager, StoreMode}
import org.rocksdb.{ColumnFamilyHandle, RocksDB}
import rapid.Task

import java.nio.charset.StandardCharsets
import java.nio.file.Path

case class RocksDBSharedStore(directory: Path) extends StoreManager {
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

  def dispose(): Task[Unit] = Task(rocksDB.closeE())
}