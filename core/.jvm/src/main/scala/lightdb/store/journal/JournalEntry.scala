package lightdb.store.journal

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.write.WriteOp

/** One store's part of a journalled commit: whether it truncates first, then its ops in order. */
case class JournalEntry[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: JournalBacked[Doc, Model],
                                                                        truncate: Boolean,
                                                                        ops: List[WriteOp[Doc]]) {
  def isEmpty: Boolean = !truncate && ops.isEmpty
}
