package lightdb.store.prefix

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store
import lightdb.transaction.PrefixScanningTransaction

trait PrefixScanningStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Store[Doc, Model] {
  override type TX <: PrefixScanningTransaction[Doc, Model]
}
