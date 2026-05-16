package lightdb.lock

import rapid.{Forge, Task}

import java.util.concurrent.ConcurrentHashMap

class LockManager[K, V] {
  private val locks = new ConcurrentHashMap[K, Lock[V]]()

  def apply(key: K, value: => Task[Option[V]], establishLock: Boolean = true)
           (f: Forge[Option[V], Option[V]]): Task[Option[V]] = if establishLock then {
    acquire(key, value).flatMap { v =>
      f(v).guarantee(release(key, v).unit)
    }
  } else {
    value.flatMap(f(_))
  }

  // Attempts to acquire a lock for a given K and V.
  def acquire(key: K, value: => Task[Option[V]]): Task[Option[V]] = Task {
    // Get or create the Lock object with the ReentrantLock.
    val lock = locks.computeIfAbsent(key, _ => new Lock(value.sync()))

    // Acquire the underlying ReentrantLock.
    lock.lock.acquire()
    lock() // Return the associated value after acquiring the lock.
  }

  // Releases the lock for the given K and supplies the latest V.
  def release(key: K, newValue: => Option[V]): Task[Option[V]] = Task {
    val v: Option[V] = newValue
    locks.compute(key, (_, existingLock) => {
      if (existingLock == null) {
        // Defensive: a prior eviction race removed the entry while a
        // holder of this key was still in flight. Releasing has
        // nothing to do (the in-flight holder's semaphore reference
        // is local; the map entry is already gone). Returning null
        // keeps the entry absent. Without this, the lambda NPE'd at
        // `existingLock.lock.release()`.
        null
      } else {
        // Decide eviction BEFORE releasing the permit. `hasQueuedThreads`
        // sees every thread currently blocked on this semaphore's
        // acquire(); any thread that called `computeIfAbsent` and got
        // this Lock object is guaranteed to be queued by the time it
        // reaches `lock.acquire()`, and the ConcurrentHashMap's atomic
        // compute lambda blocks any new `computeIfAbsent` from running
        // concurrently. So `hasQueuedThreads` here is an authoritative
        // "does anyone need this entry to stick around?" check.
        //
        // Using `availablePermits > 0` AFTER `release()` was the prior
        // bug (sigil bug #202 triggered through Strider): release()
        // unblocks a waiter who races the availablePermits read,
        // sometimes leaving the count back at 0 and producing
        // false-negative evictions where waiters' release() calls then
        // saw `existingLock = null` and NPE'd.
        val keep = existingLock.lock.hasQueuedThreads
        existingLock.lock.release()
        if (keep) {
          // Waiters are queued; keep the entry and refresh its value
          // so the next acquirer sees the latest write.
          new Lock(v, existingLock.lock)
        } else {
          // No waiters; safe to evict.
          null
        }
      }
    })
    v
  }
}
