package lightdb.transaction.handler

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.WriteHandler
import lightdb.util.AtomicQueue
import rapid.Task

class QueuedWriteHandler[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    maxQueueSize0: Int,
    flushOps: Seq[WriteOp[Doc]] => Task[Unit],
    chunkSize: Int = 5_000
) extends WriteHandler[Doc, Model] with QueueingSupport[Doc] {
  override protected val queue: AtomicQueue[WriteOp[Doc]] = new AtomicQueue[WriteOp[Doc]]
  override protected val maxQueueSize: Int = maxQueueSize0

  override protected def onOverflow: Task[Unit] = flush

  override def write(op: WriteOp[Doc]): Task[Unit] = enqueue(op)

  override def get(id: Id[Doc]): Task[Option[Option[Doc]]] = Task.pure(None)

  override def flush: Task[Unit] = Task.defer {
    def loop(): Task[Unit] = {
      val chunk = drain(chunkSize)
      if (chunk.isEmpty) Task.unit
      else flushOps(chunk).next(loop())
    }
    loop()
  }

  override def clear: Task[Unit] = Task {
    queue.clear()
  }

  override def close: Task[Unit] = flush
}
