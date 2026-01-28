package lightdb.transaction.batch

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.util.AtomicQueue
import rapid.Task

import scala.collection.compat.immutable.ArraySeq

trait QueuedBatch[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Batch[Doc, Model] {
  protected def maxQueueSize: Int = 5_000

  protected val queue = new AtomicQueue[WriteOp[Doc]]

  /**
   * Called when the maxQueueSize is reached. This may block until the queue has reduced in size, or it may start
   * async writes.
   */
  protected def overflow: Task[Unit]

  protected def write(batch: ArraySeq[WriteOp[Doc]]): Task[Unit]

  protected def write(op: WriteOp[Doc]): Task[Unit] = Task.defer {
    if (queue.add(op) > maxQueueSize) {
      overflow
    } else {
      Task.unit
    }
  }

  override def insert(doc: Doc): Task[Doc] = write(WriteOp.Insert(doc)).pure(doc)

  override def upsert(doc: Doc): Task[Doc] = write(WriteOp.Upsert(doc)).pure(doc)

  override def delete(id: Id[Doc]): Task[Unit] = write(WriteOp.Delete(id))
}
