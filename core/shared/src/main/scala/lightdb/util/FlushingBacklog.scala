package lightdb.util

import cats.effect.IO

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

abstract class FlushingBacklog[T](val batchSize: Int) {
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
    case false => IO.pure(value)
  }

  private def shouldFlush(): Boolean = synchronized {
    if (size.get() >= batchSize && !flushing.get()) {
      flushing.set(true)
      size.set(0)
      true
    } else {
      false
    }
  }

  private def prepareWrite(): IO[Unit] = IO {
    val pollingIterator = new Iterator[T] {
      override def hasNext: Boolean = !queue.isEmpty

      override def next(): T = queue.poll()
    }
    pollingIterator.toList
  }.flatMap(write)

  protected def write(list: List[T]): IO[Unit]

  def flush(): IO[Unit] = if (queue.isEmpty) {
    IO.unit
  } else {
    prepareWrite()
  }
}