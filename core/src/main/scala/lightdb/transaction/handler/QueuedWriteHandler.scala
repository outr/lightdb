package lightdb.transaction.handler

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.WriteHandler
import lightdb.util.AtomicQueue
import rapid.Task

import java.util.concurrent.ConcurrentHashMap

class QueuedWriteHandler[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    maxQueueSize0: Int,
    flushOps: Seq[WriteOp[Doc]] => Task[Unit],
    chunkSize: Int = 5_000
) extends WriteHandler[Doc, Model] with QueueingSupport[Doc] {
  override protected val queue: AtomicQueue[WriteOp[Doc]] = new AtomicQueue[WriteOp[Doc]]
  override protected val maxQueueSize: Int = maxQueueSize0

  // Read-your-writes index. The queue preserves op order for chunked flushing,
  // but `AtomicQueue.iterator` is destructive, so `get` can't scan it. This
  // id-keyed map mirrors the most recent pending op per id. An entry is removed
  // once its op has been flushed to the backend (which can then answer the read
  // itself). Without it, a `get` for an id whose write is still queued falls
  // through to the backend and wrongly reports a miss — see WriteHandler.get.
  private val pending = new ConcurrentHashMap[Id[Doc], WriteOp[Doc]]()

  override protected def onOverflow: Task[Unit] = flush

  override def write(op: WriteOp[Doc]): Task[Unit] = Task.defer {
    pending.put(op.id, op)
    enqueue(op)
  }

  override def get(id: Id[Doc]): Task[Option[Option[Doc]]] = Task {
    pending.get(id) match {
      case null => None
      case WriteOp.Delete(_) => Some(None)
      case WriteOp.Insert(doc) => Some(Some(doc))
      case WriteOp.Upsert(doc) => Some(Some(doc))
    }
  }

  override def flush: Task[Unit] = Task.defer {
    def loop(): Task[Unit] = {
      val chunk = drain(chunkSize)
      if (chunk.isEmpty) Task.unit
      else flushOps(chunk).map { _ =>
        // The chunk is now persisted in the backend, so drop its index
        // entries. The conditional remove leaves a newer same-id op (queued
        // after this chunk was drained) in place so its pending value wins.
        chunk.foreach(op => pending.remove(op.id, op))
      }.next(loop())
    }
    loop()
  }

  override def clear: Task[Unit] = Task {
    queue.clear()
    pending.clear()
  }

  override def close: Task[Unit] = flush
}
