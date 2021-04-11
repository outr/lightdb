package lightdb.util

import cats.effect.{IO, Resource}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future, Promise}

object ObjectLock {
  private val map = new ConcurrentHashMap[Any, List[ReleasableLock => Unit]]

  def isEmpty: Boolean = map.isEmpty

  def apply[Key](key: Key, f: ReleasableLock => Unit): Unit = {
    var trigger = false
    map.compute(key, (_, q) => {
      val existingQueue = Option(q).getOrElse(Nil)
      trigger = existingQueue.isEmpty
      val queue = existingQueue ::: List(f)
      queue
    })
    if (trigger) {
      triggerNext(key)
    }
  }

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

  private def triggerNext[Key](key: Key): Unit = {
    map.compute(key, (_, q) => {
      val queue = Option(q).getOrElse(Nil)
      if (queue.nonEmpty) {
        val task = queue.head
        val lock = new Lock(key)
        task(lock)

        val updatedQueue = queue.tail
        if (updatedQueue.isEmpty) {
          None.orNull
        } else {
          updatedQueue
        }
      } else {
        None.orNull
      }
    })
  }

  trait ReleasableLock {
    def release(): Unit
  }

  class Lock[Key](key: Key) extends ReleasableLock {
    override def release(): Unit = triggerNext(key)
  }
}