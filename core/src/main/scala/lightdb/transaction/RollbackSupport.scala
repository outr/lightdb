package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}
import rapid.Task

trait RollbackSupport[Doc <: Document[Doc], Model <: DocumentModel[Doc]] { self: Transaction[Doc, Model] =>
  def rollback: Task[Unit] = writeHandler.clear.next(_rollback).next(Task {
    // Discard any pending cache mutations — rolled-back writes must never reach the store cache.
    cachePending.clear()
  })
}
