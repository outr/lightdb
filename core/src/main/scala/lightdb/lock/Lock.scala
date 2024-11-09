package lightdb.lock

import java.util.concurrent.Semaphore

class Lock[V](value: => Option[V], val lock: Semaphore = new Semaphore(1, true)) {
  private lazy val v: Option[V] = value

  def apply(): Option[V] = v
}