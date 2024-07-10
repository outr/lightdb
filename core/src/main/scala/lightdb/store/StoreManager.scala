package lightdb.store

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}

trait StoreManager {
  def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                               name: String,
                                               storeMode: StoreMode): Store[Doc, Model]
}