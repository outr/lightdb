package lightdb.util

import cats.effect.{IO, Resource}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future, Promise}

trait AbstractObjectLock {
  def isEmpty: Boolean
  def apply[Key](key: Key, f: ReleasableLock => Unit): Unit

  def future[Key, Return](key: Key)(f: => Future[Return])(implicit ec: ExecutionContext): Future[Return] = {
    val promise = Promise[Return]()
    apply(key, (lock: ReleasableLock) => {
      f.onComplete { result =>
        promise.complete(result)
        lock.release()
      }
    })
    promise.future
  }

  def io[Key, Return](key: Key)(task: IO[Return]): IO[Return] = resource(key).use(_ => task)

  def resource[Key](key: Key): Resource[IO, Unit] = for {
    lock <- Resource.eval(IO.async_[ReleasableLock] { callback =>
      apply(key, (lock: ReleasableLock) => {
        callback(Right(lock))
      })
    })
    _ <- Resource.make(IO.unit)(_ => IO(lock.release()))
  } yield ()

  protected def triggerNext[Key](key: Key): Unit

  trait ReleasableLock {
    def release(): Unit
  }

  class Lock[Key](key: Key) extends ReleasableLock {
    override def release(): Unit = triggerNext(key)
  }
}