package lightdb.util

import cats.effect.IO

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.DurationInt

abstract class FlushingBacklog[T](val batchSize: Int, val maxSize: Int) {
  private val queue = new ConcurrentLinkedQueue[T]()
  private val size = new AtomicInteger(0)
  private val flushing = new AtomicBoolean(false)

  def enqueue(value: T): IO[T] = IO {
    queue.add(value)
    val size = this.size.incrementAndGet()
    var doFlush = false
    if (size >= batchSize) {
      doFlush = shouldFlush()
    }
    doFlush
  }.flatMap {
    case true => prepareWrite().map(_ => value)
    case false => waitForBuffer().map(_ => value)
  }

  private def waitForBuffer(): IO[Unit] = if (size.get() > maxSize) {
    IO.sleep(1.second).flatMap(_ => waitForBuffer())
  } else {
    IO.unit
  }

  private def shouldFlush(): Boolean = synchronized {
    if (size.get() >= batchSize && !flushing.get()) {
      flushing.set(true)
      true
    } else {
      false
    }
  }

  private def prepareWrite(): IO[Unit] = IO {
    val pollingIterator = new Iterator[T] {
      override def hasNext: Boolean = !queue.isEmpty

      override def next(): T = {
        val value = queue.poll()
        if (value != null) {
          FlushingBacklog.this.size.decrementAndGet()
        }
        value
      }
    }
    pollingIterator.toList
  }.flatMap(writeBatched).map(_ => flushing.set(false))

  private def writeBatched(list: List[T]): IO[Unit] = {
    val (current, more) = list.splitAt(batchSize)
    val w = write(current)
    if (more.nonEmpty) {
      w.flatMap(_ => writeBatched(more))
    } else {
      w
    }
  }

  protected def write(list: List[T]): IO[Unit]

  def flush(): IO[Unit] = if (queue.isEmpty) {
    IO.unit
  } else {
    prepareWrite()
  }
}