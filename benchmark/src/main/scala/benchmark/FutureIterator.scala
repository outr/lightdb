package benchmark

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}

trait FutureIterator[T] {
  val running = new AtomicInteger(0)

  def next()(implicit ec: ExecutionContext): Future[Option[T]]

  def stream(concurrency: Int)(f: T => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = {
    val futures = (0 until concurrency).map { _ =>
      running.incrementAndGet()
      recursiveStream(f)
    }
    Future.sequence(futures).map(_ => ())
  }

  private def recursiveStream(f: T => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = next().flatMap {
    case Some(t) => f(t).flatMap { _ =>
      recursiveStream(f)
    }
    case None => {
      running.decrementAndGet()
      Future.unit
    }
  }
}