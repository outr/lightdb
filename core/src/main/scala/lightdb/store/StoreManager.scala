package lightdb.store

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}

trait StoreManager {
  lazy val name: String = getClass.getSimpleName.replace("$", "")

  def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                               name: String,
                                               storeMode: StoreMode): Store[Doc, Model]
}