package lightdb.store

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.write.WriteOp

/** One store's writes in an atomic write ([[AtomicWriteStore.writeAtomically]]). */
case class AtomicBatch[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: AtomicWriteStore[Doc, Model], ops: List[WriteOp[Doc]])
