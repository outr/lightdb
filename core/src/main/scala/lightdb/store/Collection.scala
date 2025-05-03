package lightdb.store

import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.transaction.CollectionTransaction

import java.nio.file.Path

abstract class Collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                             path: Option[Path],
                                                                             model: Model,
                                                                             lightDB: LightDB,
                                                                             storeManager: StoreManager) extends Store[Doc, Model](name, path, model, lightDB, storeManager) {
  override type TX <: CollectionTransaction[Doc, Model]
}
