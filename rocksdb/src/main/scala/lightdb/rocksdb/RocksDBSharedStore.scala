package lightdb.rocksdb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}
import org.rocksdb.{ColumnFamilyHandle, RocksDB}
import rapid.Task

import java.nio.file.Path

case class RocksDBSharedStore(directory: Path) extends StoreManager {
  RocksDB.loadLibrary()

  val (rocksDB: RocksDB, existingHandles: List[ColumnFamilyHandle]) = RocksDBStore.createRocksDB(directory)

  val existingHandlesMap: Map[String, ColumnFamilyHandle] = existingHandles.map { h =>
    new String(h.getName, "UTF-8") -> h
  }.toMap

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] =
    new RocksDBStore[Doc, Model](
      name = name,
      model = model,
      rocksDB = rocksDB,
      sharedStore = Some(RocksDBSharedStoreInstance(this, name)),
      storeMode = storeMode,
      storeManager = this
    )

  def dispose(): Task[Unit] = Task(rocksDB.closeE())
}