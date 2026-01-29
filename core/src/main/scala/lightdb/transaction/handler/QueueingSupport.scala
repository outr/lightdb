package lightdb.transaction.handler

import lightdb.doc.Document
import lightdb.store.write.WriteOp
import lightdb.util.AtomicQueue
import rapid.Task

import scala.collection.compat.immutable.ArraySeq
import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait QueueingSupport[Doc <: Document[Doc]] {
  protected def queue: AtomicQueue[WriteOp[Doc]]
  protected def maxQueueSize: Int
  protected def overflowDelay: FiniteDuration = 50.millis

  protected def enqueue(op: WriteOp[Doc]): Task[Unit] = Task.defer {
    if (queue.add(op) > maxQueueSize) onOverflow else Task.unit
  }

  protected def onOverflow: Task[Unit] =
    Task.condition(Task.function(queue.size < maxQueueSize), delay = overflowDelay)

  protected def drain(max: Int): ArraySeq[WriteOp[Doc]] = queue.poll(max)
}
