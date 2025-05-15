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

  val existingHandlesMap: Map[String, ColumnFamilyHandle] = existingHandles.map { h =>
    new String(h.getName, StandardCharsets.UTF_8) -> h
  }.toMap

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