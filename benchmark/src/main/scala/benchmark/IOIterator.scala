package benchmark

import rapid.Task

import java.util.concurrent.atomic.AtomicInteger

trait IOIterator[T] {
  val running = new AtomicInteger(0)

  def next(): Task[Option[T]]

  def stream(concurrency: Int)(f: T => Task[Unit]): Task[Unit] = {
    val ios = (0 until concurrency).toList.map { _ =>
      running.incrementAndGet()
      recursiveStream(f)
    }
    ios.tasks.map(_ => ())
  }

  private def recursiveStream(f: T => Task[Unit]): Task[Unit] = next().flatMap {
    case Some(t) => f(t).flatMap { _ =>
      recursiveStream(f)
    }
    case None => {
      running.decrementAndGet()
      Task.unit
    }
  }
}