package lightdb.rocksdb

import lightdb._
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{PrefixScanningStore, PrefixScanningStoreManager, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, DBOptions, FlushOptions, Options, RocksDB}
import rapid.Task

import java.nio.file.{Files, Path}
import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

class RocksDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                      path: Option[Path],
                                                                      model: Model,
                                                                      private[rocksdb] val rocksDB: RocksDB,
                                                                      sharedStore: Option[RocksDBSharedStoreInstance],
                                                                      val storeMode: StoreMode[Doc, Model],
                                                                      lightDB: LightDB,
                                                                      storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) with PrefixScanningStore[Doc, Model] {
  override type TX = RocksDBTransaction[Doc, Model]

  private[rocksdb] val handle: Option[ColumnFamilyHandle] = sharedStore.map { ss =>
    ss.existingHandle match {
      case Some(handle) => handle
      case None => rocksDB.createColumnFamily(new ColumnFamilyDescriptor(ss.handle.getBytes("UTF-8")))
    }
  }

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    RocksDBTransaction(this, parent)
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    val o = new FlushOptions
    handle match {
      case Some(h) =>
        rocksDB.flush(o, h)
      case None =>
        rocksDB.flush(o)
        rocksDB.closeE()
    }
  })
}

object RocksDBStore extends PrefixScanningStoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = RocksDBStore[Doc, Model]

  def createRocksDB(directory: Path): (RocksDB, List[ColumnFamilyHandle]) = {
    RocksDB.loadLibrary()

    Files.createDirectories(directory.getParent)
    val path = directory.toAbsolutePath.toString
    val columnFamilies = new util.ArrayList[ColumnFamilyDescriptor]
    columnFamilies.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY))
    RocksDB.listColumnFamilies(new Options(), path)
      .asScala
      .foreach { name =>
        columnFamilies.add(new ColumnFamilyDescriptor(name))
      }
    val handles = new util.ArrayList[ColumnFamilyHandle]()
    val options = new DBOptions()
      .setCreateIfMissing(true)
    RocksDB.open(options, path, columnFamilies, handles) -> handles.asScala.toList
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): RocksDBStore[Doc, Model] =
    new RocksDBStore[Doc, Model](
      name = name,
      path = path,
      model = model,
      rocksDB = createRocksDB(path.get)._1,
      sharedStore = None,
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )
}
