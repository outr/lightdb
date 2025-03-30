package lightdb.store

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}

trait StoreManager {
  lazy val name: String = getClass.getSimpleName.replace("$", "")

  type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] <: Store[Doc, Model]

  def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                model: Model,
                                                                name: String,
                                                                storeMode: StoreMode[Doc, Model]): S[Doc, Model]
}
