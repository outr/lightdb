package lightdb.store.split

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{CollectionManager, Store, StoreManager, StoreMode}

case class SplitStoreManager(storage: StoreManager,
                             searching: CollectionManager,
                             searchIndexAll: Boolean = false) extends CollectionManager {
  override lazy val name: String = s"Split($storage, $searching)"

  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = SplitCollection[Doc, Model]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): SplitCollection[Doc, Model] = {
    val storage = this.storage.create[Doc, Model](db, model, name, StoreMode.All())
    new SplitCollection(
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
