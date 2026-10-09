package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}

/**
 * A transaction on a store that writes through (each write durable when made, with no backend rollback) that can
 * still be rolled back: once [[recordUndo]] is called it keeps the row each id held before its first write, and its
 * rollback puts those rows back. A truncate is not undone.
 */
trait UndoableTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model] {
  def recordUndo(): Unit
}
