package lightdb.lock

import rapid.{Forge, Task}

import java.util.concurrent.ConcurrentHashMap

class LockManager[K, V] {
  private val locks = new ConcurrentHashMap[K, Lock[V]]()

  def apply(key: K, value: => Option[V], establishLock: Boolean = true)
           (f: Forge[Option[V], Option[V]]): Task[Option[V]] = if (establishLock) {
    acquire(key, value).flatMap { v =>
      f(v).guarantee(release(key, v).unit)
    }
  } else {
    f(value)
  }

  // Attempts to acquire a lock for a given K and V.
  def acquire(key: K, value: => Option[V]): Task[Option[V]] = Task {
    // Get or create the Lock object with the ReentrantLock.
    val lock = locks.computeIfAbsent(key, _ => new Lock(value))

    // Acquire the underlying ReentrantLock.
    lock.lock.acquire()
    lock() // Return the associated value after acquiring the lock.
  }

  // Releases the lock for the given K and supplies the latest V.
  def release(key: K, newValue: => Option[V]): Task[Option[V]] = Task {
    val v: Option[V] = newValue
    locks.compute(key, (_, existingLock) => {
      // Update the value associated with the lock.
      existingLock.lock.release()

      if (existingLock.lock.availablePermits() > 0) {
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
