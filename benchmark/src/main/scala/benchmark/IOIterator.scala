package benchmark

import cats.effect.IO
import cats.implicits._

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}

trait IOIterator[T] {
  val running = new AtomicInteger(0)

  def next(): IO[Option[T]]

  def stream(concurrency: Int)(f: T => IO[Unit]): IO[Unit] = {
    val ios = (0 until concurrency).toList.map { _ =>
      running.incrementAndGet()
      recursiveStream(f)
    }
    ios.sequence.map(_ => ())
  }

  private def recursiveStream(f: T => IO[Unit]): IO[Unit] = next().flatMap {
    case Some(t) => f(t).flatMap { _ =>
      recursiveStream(f)
    }
    case None => {
      running.decrementAndGet()
      IO.unit
    }
  }
}