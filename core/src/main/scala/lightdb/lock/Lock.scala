package lightdb.lock

import java.util.concurrent.locks.ReentrantLock

class Lock[V](value: => Option[V], val lock: ReentrantLock) {
  private lazy val v: Option[V] = value

  def apply(): Option[V] = v
}