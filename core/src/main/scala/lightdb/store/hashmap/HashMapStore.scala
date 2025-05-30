package lightdb.store.hashmap

import lightdb._
import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import rapid.Task

import java.nio.file.Path

class HashMapStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                      path: Option[Path],
                                                                      model: Model,
                                                                      val storeMode: StoreMode[Doc, Model],
                                                                      db: LightDB,
                                                                      storeManager: StoreManager) extends Store[Doc, Model](name, path, model, db, storeManager) { store =>
  override type TX = HashMapTransaction[Doc, Model]

  private[store] var _map = Map.empty[Id[Doc], Doc]

  def map: Map[Id[Doc], Doc] = _map

  override protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX] = Task(HashMapTransaction(this, parent))

  override protected def doDispose(): Task[Unit] = Task {
    store.synchronized {
      _map = Map.empty
    }
  }
}

object HashMapStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = HashMapStore[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): HashMapStore[Doc, Model] = new HashMapStore[Doc, Model](name, path, model, storeMode, db, this)
}
