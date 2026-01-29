package lightdb.transaction.handler

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.WriteHandler
import lightdb.util.AtomicQueue
import rapid.{Task, logger}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.FiniteDuration

class AsyncWriteHandler[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    activeThreads: Int,
    chunkSize: Int,
    waitTime: FiniteDuration,
    maxQueueSize0: Int,
    flushOps: Seq[WriteOp[Doc]] => Task[Unit]
) extends WriteHandler[Doc, Model] with QueueingSupport[Doc] {
  override protected val queue: AtomicQueue[WriteOp[Doc]] = new AtomicQueue[WriteOp[Doc]]
  override protected val maxQueueSize: Int = maxQueueSize0

  @volatile private var keepAlive = true
  @volatile private var throwable: Option[Throwable] = None
  private val finished = new AtomicInteger(0)

  private val active = (0 until activeThreads).map { index =>
    recursivelyProcess.handleError { t =>
      throwable = Some(t)
      logger.error(s"Error in AsyncWriteHandler.active[$index]", t).map(_ => keepAlive = false)
    }.guarantee(Task {
      finished.incrementAndGet()
    }).start()
  }

  override protected def onOverflow: Task[Unit] = super.onOverflow

  override def write(op: WriteOp[Doc]): Task[Unit] = enqueue(op)

  override def get(id: Id[Doc]): Task[Option[Option[Doc]]] = Task.pure(None)

  private def recursivelyProcess: Task[Unit] = Task.defer {
    val chunk = drain(chunkSize)
    if ((chunk.isEmpty && !keepAlive) || throwable.nonEmpty) {
      Task.unit
    } else {
      val task = if (chunk.nonEmpty) {
        flushOps(chunk)
      } else {
        Task.sleep(waitTime)
      }
      task.next(recursivelyProcess)
    }
  }

  override def flush: Task[Unit] = Task.defer {
    val chunk = drain(chunkSize)
    if (chunk.isEmpty) Task.unit
    else flushOps(chunk).next(flush)
  }

  override def clear: Task[Unit] = Task {
    queue.clear()
  }

  override def close: Task[Unit] = Task.defer {
    keepAlive = false
    Task.condition(Task.function(finished.get() == activeThreads), delay = waitTime)
  }.next(flush).function {
    throwable.foreach(t => throw t)
  }
}
