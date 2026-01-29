package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import rapid.Task

trait WriteHandler[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  /** Handle a write operation (may buffer, queue, or write directly) */
  def write(op: WriteOp[Doc]): Task[Unit]

  /** Check for a document in buffer (for read-your-writes support).
    * Returns: None = not buffered, Some(None) = deleted, Some(Some(doc)) = buffered
    */
  def get(id: Id[Doc]): Task[Option[Option[Doc]]]

  /** Flush any pending writes to the store */
  def flush: Task[Unit]

  /** Clear pending writes (for rollback) */
  def clear: Task[Unit]

  /** Await any async workers and cleanup */
  def close: Task[Unit]
}
