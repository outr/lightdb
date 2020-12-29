package testdb

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future, Promise}

object ObjectTaskQueue {
  private val map = new ConcurrentHashMap[Any, List[() => Future[Unit]]]

  def isEmpty: Boolean = map.isEmpty

  def apply[Key, Return](key: Key)
                        (f: => Future[Return])
                        (implicit ec: ExecutionContext): Future[Return] = {
    val promise = Promise[Return]
    map.compute(key, (_, t) => {
      val newTask = () => f.transformWith { result =>
        result.failed.foreach(errorHandler(key, _))
        clearHead(key)
        promise.complete(result)
        Future.successful(())
      }
      val queue = Option(t).getOrElse(Nil)
      try {
        queue ::: List(newTask)
      } finally {
        if (queue.isEmpty) {
          newTask()
        }
      }
    })
    promise.future
  }

  private def clearHead[Key](key: Key): Unit = map.compute(key, (_, tasks) => {
    val queue = tasks.tail
    if (queue.isEmpty) {
      None.orNull
    } else {
      val next = queue.head
      next()
      queue
    }
  })

  protected def errorHandler[Key](key: Key, throwable: Throwable): Unit = {
    println(s"Error occurred while handling: $key")
    throwable.printStackTrace()
  }
}