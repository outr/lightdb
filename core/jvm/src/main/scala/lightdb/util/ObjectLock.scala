package lightdb.util

import cats.effect.{IO, Resource}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future, Promise}

object ObjectLock extends AbstractObjectLock {
  private val map = new ConcurrentHashMap[Any, List[ReleasableLock => Unit]]

  override def isEmpty: Boolean = map.isEmpty

  override def apply[Key](key: Key, f: ReleasableLock => Unit): Unit = {
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

  override protected def triggerNext[Key](key: Key): Unit = {
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
}
