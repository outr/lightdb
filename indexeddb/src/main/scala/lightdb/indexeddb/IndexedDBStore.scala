package lightdb.indexeddb

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import org.scalajs.dom.IDBDatabase
import rapid.Task

import java.nio.file.Path

/**
 * A key-value LightDB store persisted in the browser's IndexedDB.
 *
 * Each store is its own IndexedDB database (`<db name>/<store name>`) holding one object store of documents serialized
 * as JSON and keyed by id. Like `HashMapStore` it supports id lookups, streaming, counting and deletes, but not
 * filtered queries.
 */
class IndexedDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                        path: Option[Path],
                                                                        model: Model,
                                                                        val storeMode: StoreMode[Doc, Model],
                                                                        lightDB: LightDB,
                                                                        storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX = IndexedDBTransaction[Doc, Model]

  /** The IndexedDB database name. Scoped by the LightDB name so two databases can share a store name. */
  val databaseName: String = s"${lightDB.name}/$name"

  @volatile private var _db: IDBDatabase = null

  private[indexeddb] def db: IDBDatabase = {
    if (_db == null) throw new IllegalStateException(s"IndexedDB store $databaseName is not initialized")
    _db
  }

  override def defaultBatchConfig: BatchConfig = BatchConfig.Direct

  override protected def initialize(): Task[Unit] = super.initialize().next(IDB.open(databaseName).map(db => _db = db))

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] =
    Task(IndexedDBTransaction(this, parent, writeHandlerFactory))

  override protected def doDispose(): Task[Unit] = super.doDispose().guarantee(Task {
    if (_db != null) _db.close()
    _db = null
  })
}

object IndexedDBStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = IndexedDBStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): IndexedDBStore[Doc, Model] =
    new IndexedDBStore[Doc, Model](name, path, model, storeMode, db, this)

  /** Permanently delete a store's IndexedDB database. The store must be disposed first. */
  def deleteDatabase(databaseName: String): Task[Unit] = IDB.delete(databaseName)
}
