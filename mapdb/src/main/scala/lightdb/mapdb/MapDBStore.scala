package lightdb.mapdb

import lightdb.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.prefix.{PrefixScanningStore, PrefixScanningStoreManager}
import lightdb.store.{Store, StoreMode}
import lightdb.transaction.Transaction
import org.mapdb.{DB, DBMaker, BTreeMap, Serializer}
import rapid.Task

import java.nio.file.{Files, Path}

class MapDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                    path: Option[Path],
                                                                    model: Model,
                                                                    val storeMode: StoreMode[Doc, Model],
                                                                    lightDB: LightDB,
                                                                    storeManager: PrefixScanningStoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) with PrefixScanningStore[Doc, Model] {
  override type TX = MapDBTransaction[Doc, Model]

  private lazy val db: DB = {
    val maker = path.map { path =>
      Files.createDirectories(path.getParent)
      DBMaker.fileDB(path.toFile)
    }.getOrElse(DBMaker.memoryDirectDB())
    maker.make()
  }
  private[mapdb] lazy val map: BTreeMap[String, String] = db
    .treeMap("map", Serializer.STRING, Serializer.STRING)
    .createOrOpen()

  override protected def initialize(): Task[Unit] = super.initialize()

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task {
    MapDBTransaction(this, parent)
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    db.commit()
    db.close()
  })
}

object MapDBStore extends PrefixScanningStoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = MapDBStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] =
    new MapDBStore[Doc, Model](name, path, model, storeMode, db, this)
}