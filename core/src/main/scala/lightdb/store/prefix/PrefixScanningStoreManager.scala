package lightdb.store.prefix

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.StoreManager

trait PrefixScanningStoreManager extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] <: PrefixScanningStore[Doc, Model]
}
