package lightdb.lock

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

class LockManager[K, V] {
  private val locks = new ConcurrentHashMap[K, Lock[V]]()

  def apply(key: K, value: => Option[V], establishLock: Boolean = true)
           (f: Option[V] => Option[V]): Option[V] = if (establishLock) {
    val v = acquire(key, value)
    try {
      val modified = f(v)
      release(key, modified)
    } catch {
      case t: Throwable =>
        release(key, v)
        throw t
    }
  } else {
    f(value)
  }

  // Attempts to acquire a lock for a given K and V.
  def acquire(key: K, value: => Option[V]): Option[V] = {
    // Get or create the Lock object with the ReentrantLock.
    val lock = locks.computeIfAbsent(key, _ => new Lock(value, new ReentrantLock))

    // Acquire the underlying ReentrantLock.
    lock.lock.lock()
    lock() // Return the associated value after acquiring the lock.
  }

  // Releases the lock for the given K and supplies the latest V.
  def release(key: K, newValue: => Option[V]): Option[V] = {
    val v: Option[V] = newValue
    locks.compute(key, (_, existingLock) => {
      // Update the value associated with the lock.
      existingLock.lock.unlock()

      if (!existingLock.lock.hasQueuedThreads) {
        // No other threads are waiting, so remove the lock entry.
        null
      } else {
        // Other threads are waiting, so update the value but keep the lock.
        new Lock(v, existingLock.lock)
      }
    })
    v
  }
}
