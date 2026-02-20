package lightdb.transaction.handler

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.WriteHandler
import lightdb.util.AtomicQueue
import rapid.{Task, logger}

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
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
  private val throwable = new AtomicReference[Throwable](null)
  private val finished = new AtomicInteger(0)
  private val minChunkSize = math.max(1, chunkSize / 4)
  private val maxChunkSize = math.max(chunkSize, chunkSize * 4)

  private val active = (0 until activeThreads).map { index =>
    recursivelyProcess.handleError { t =>
      Task {
        throwable.compareAndSet(null, t)
        keepAlive = false
      }.next(logger.error(s"Error in AsyncWriteHandler.active[$index]", t))
    }.guarantee(Task {
      finished.incrementAndGet()
    }).start()
  }

  override protected def onOverflow: Task[Unit] = super.onOverflow

  private def failIfError: Task[Unit] = Task.defer {
    val t = throwable.get()
    if (t == null) Task.unit else Task.error(t)
  }

  override def write(op: WriteOp[Doc]): Task[Unit] = failIfError.next(enqueue(op))

  override def get(id: Id[Doc]): Task[Option[Option[Doc]]] = Task.pure(None)

  /**
   * Adapt chunk size to queue depth so bursty writers flush larger batches,
   * while light traffic still gets low-latency small flushes.
   */
  private def adaptiveChunkSize(): Int = {
    val qSize = queue.size
    if (qSize <= 0) {
      chunkSize
    } else {
      val perThreadBacklog = math.max(1, qSize / math.max(1, activeThreads))
      math.max(minChunkSize, math.min(maxChunkSize, math.max(chunkSize, perThreadBacklog)))
    }
  }

  private def recursivelyProcess: Task[Unit] = Task.defer {
    val chunk = drain(adaptiveChunkSize())
    if ((chunk.isEmpty && !keepAlive) || throwable.get() != null) {
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
    failIfError.next {
      val chunk = drain(adaptiveChunkSize())
      if (chunk.isEmpty) Task.unit
      else flushOps(chunk).next(flush)
    }
  }

  override def clear: Task[Unit] = Task {
    queue.clear()
  }

  override def close: Task[Unit] = Task.defer {
    failIfError.next(Task {
      keepAlive = false
    }).next(Task.condition(Task.function(finished.get() == activeThreads), delay = waitTime)).next {
      failIfError.next(flush)
    }
  }.function {
    val t = throwable.get()
    if (t != null) throw t
  }
}
