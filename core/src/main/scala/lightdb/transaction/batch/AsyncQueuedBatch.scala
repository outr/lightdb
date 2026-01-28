package lightdb.transaction.batch

import lightdb.doc.{Document, DocumentModel}
import rapid.{Task, logger}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.{DurationInt, FiniteDuration}

trait AsyncQueuedBatch[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends QueuedBatch[Doc, Model] {
  protected def activeThreads: Int = 4

  protected def chunkSize: Int = 5_000

  protected def waitTime: FiniteDuration = 250.millis

  override protected def maxQueueSize: Int = 20_000

  @volatile private var keepAlive = true
  @volatile private var throwable = Option.empty[Throwable]
  private val finished = new AtomicInteger(0)

  private val active = (0 until activeThreads).map { index =>
    recursivelyProcess.handleError { throwable =>
      this.throwable = Some(throwable)
      logger.error(s"Error in Batch.active[$index]", throwable).map(_ => keepAlive = false)
    }.guarantee(Task {
      finished.incrementAndGet()
    }).start()
  }

  protected def recursivelyProcess: Task[Unit] = Task.defer {
    val chunk = queue.poll(chunkSize)
    if ((chunk.isEmpty && !keepAlive) || throwable.nonEmpty) {
      Task.unit
    } else {
      val task = if (chunk.nonEmpty) {
        write(chunk)
      } else {
        Task.sleep(waitTime)
      }
      task.next(recursivelyProcess)
    }
  }

  override protected def overflow: Task[Unit] = Task.condition(Task.function(queue.size < maxQueueSize), delay = 50.millis)

  override def close: Task[Unit] = Task.defer {
    keepAlive = false
    Task.condition(Task.function(finished.get() == activeThreads), delay = 10.millis)
  }.next(super.close).function {
    throwable.foreach(t => throw t)
  }
}
