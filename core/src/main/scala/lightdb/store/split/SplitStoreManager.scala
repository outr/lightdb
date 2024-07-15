package lightdb.store.split

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}

case class SplitStoreManager(storage: StoreManager,
                             searching: StoreManager,
                             searchingMode: StoreMode = StoreMode.All) extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                        name: String,
                                                        storeMode: StoreMode): Store[Doc, Model] = SplitStore(
    storage = storage.create[Doc, Model](db, name, StoreMode.All),
    searching = searching.create[Doc, Model](db, name, searchingMode),
    storeMode = storeMode
  )
}
