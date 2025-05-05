package lightdb.store.split

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{CollectionManager, StoreManager, StoreMode}

import java.nio.file.Path

case class SplitStoreManager[Storage <: StoreManager, Searching <: CollectionManager](storage: Storage,
                                                                                      searching: Searching,
                                                                                      searchIndexAll: Boolean = false) extends CollectionManager {
  override lazy val name: String = s"Split($storage, $searching)"

  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = SplitCollection[Doc, Model, storage.S[Doc, Model], searching.S[Doc, Model]]

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    val storage = this.storage.create[Doc, Model](db, model, name, path.map(_.resolve("storage")), StoreMode.All())
    new SplitCollection(
      name = name,
      model = model,
      storage = storage,
      searching = searching.create[Doc, Model](db, model, name, path.map(_.resolve("search")), if (searchIndexAll) StoreMode.All() else StoreMode.Indexes(storage)),
      storeMode = storeMode,
      db = db,
      path = path,
      storeManager = this
    )
  }
}
