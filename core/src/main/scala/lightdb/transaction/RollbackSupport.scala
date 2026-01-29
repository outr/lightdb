package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}
import rapid.Task

trait RollbackSupport[Doc <: Document[Doc], Model <: DocumentModel[Doc]] { self: Transaction[Doc, Model] =>
  def rollback: Task[Unit] = writeHandler.clear.next(_rollback)
}
