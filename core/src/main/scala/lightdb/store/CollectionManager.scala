package lightdb.store

import lightdb.doc.{Document, DocumentModel}

trait CollectionManager extends StoreManager {
  type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] <: Collection[Doc, Model]
}
