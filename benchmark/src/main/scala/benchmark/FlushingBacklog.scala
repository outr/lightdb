package benchmark

import cats.effect.IO

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

abstract class FlushingBacklog[Key, Value](val batchSize: Int, val maxSize: Int) {
  private val map = new ConcurrentHashMap[Key, Value]
  private val size = new AtomicInteger(0)
  private val flushing = new AtomicBoolean(false)

  def enqueue(key: Key, value: Value): IO[Value] = IO.blocking {
    val exists = map.put(key, value) != null
    var doFlush = false
    if (!exists) {
      val size = this.size.incrementAndGet()
      if (size >= batchSize) {
        doFlush = shouldFlush()
      }
    }
    doFlush
  }.flatMap {
    case true => prepareWrite().map(_ => value)
    case false => waitForBuffer().map(_ => value)
  }

  def remove(key: Key): Boolean = {
    val b = map.remove(key) != null
    if (b) {
      size.decrementAndGet()
    }
    b
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

  private def pollingStream: fs2.Stream[IO, Value] = fs2.Stream
    .fromBlockingIterator[IO](map.keys().asIterator().asScala, 512)
    .map { key =>
      val o = Option(map.remove(key))
      if (o.nonEmpty) {
        val s = size.decrementAndGet()
        if (s < 0) {
          scribe.warn("Size fell below zero!")
          size.set(0)
        }
      }
      o
    }
    .unNone

  private def prepareWrite(): IO[Unit] = pollingStream
    .compile
    .toList
    .flatMap { list =>
      writeBatched(list)
    }
    .map { _ =>
      flushing.set(false)
    }

  private def writeBatched(list: List[Value]): IO[Unit] = {
    val (current, more) = list.splitAt(batchSize)
    val w = write(current)
    if (more.nonEmpty) {
      w.flatMap(_ => writeBatched(more))
    } else {
      w
    }
  }

  protected def write(list: List[Value]): IO[Unit]

  def flush(): IO[Unit] = if (map.isEmpty) {
    IO.unit
  } else {
    prepareWrite()
  }
}