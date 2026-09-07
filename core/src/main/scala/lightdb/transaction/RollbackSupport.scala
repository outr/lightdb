package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}
/** Compatibility marker. The lifecycle now lives in Transaction so all wrappers discard failed
  * pending work. Actual rollback of already-written data still depends on the backend. */
trait RollbackSupport[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model]
