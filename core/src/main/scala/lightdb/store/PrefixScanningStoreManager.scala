package lightdb.store

import lightdb.doc.{Document, DocumentModel}

trait PrefixScanningStoreManager extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] <: PrefixScanningStore[Doc, Model]
}
