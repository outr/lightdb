package benchmark

import cats.effect.IO
import cats.implicits._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}

trait IOIterator[T] {
  val running = new AtomicInteger(0)

  def next(): Task[Option[T]]

  def stream(concurrency: Int)(f: T => Task[Unit]): Task[Unit] = {
    val ios = (0 until concurrency).toList.map { _ =>
      running.incrementAndGet()
      recursiveStream(f)
    }
    ios.sequence.map(_ => ())
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