package lightdb.transaction.batch

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid.Task

trait Batch[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  protected def transaction: Transaction[Doc, Model]

  def insert(doc: Doc): Task[Doc]
  def upsert(doc: Doc): Task[Doc]
  def delete(id: Id[Doc]): Task[Unit]

  def flush: Task[Int]
  
  protected def _dispose: Task[Unit] = Task.unit
  
  def close: Task[Unit] = flush.next(Transaction.releaseBatch(transaction, this)).next(_dispose)
}