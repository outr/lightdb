package lightdb.store.split

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}

case class SplitStoreManager(storage: StoreManager,
                             searching: StoreManager,
                             searchIndexAll: Boolean = false) extends StoreManager {
  override lazy val name: String = s"Split($storage, $searching)"

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] = {
    val storage = this.storage.create[Doc, Model](db, model, name, StoreMode.All())
    new SplitStore(
      name = name,
      model = model,
      storage = storage,
      searching = searching.create[Doc, Model](db, model, name, if (searchIndexAll) StoreMode.All() else StoreMode.Indexes(storage)),
      storeMode = storeMode,
      db = db,
      storeManager = this
    )
  }
}
