package lightdb.lock

import rapid.{Forge, Task}

import java.util.concurrent.{ConcurrentHashMap, Semaphore}

/**
 * Per-key mutual exclusion within one process: at most one holder per key at a time, waiters served in arrival order.
 *
 * The manager keeps no values. A holder reads the value it works on after it has taken the key ([[apply]],
 * [[acquire]]), so it sees what the previous holder left rather than a copy taken before it waited.
 *
 * The lock lives in this JVM only: it excludes nothing in another process or on another replica using the same
 * database.
 */
class LockManager[K, V] {
  // `users` counts the threads holding or waiting for the key. It changes only inside the map's atomic `compute`, so
  // an entry is removed exactly when no thread holds it or is about to wait on it.
  private final class Entry {
    val semaphore = new Semaphore(1, true)
    var users: Int = 0
  }

  private val locks = new ConcurrentHashMap[K, Entry]()

  /** Runs `f` holding `key`, on the value `value` reads once the key is held. */
  def apply(key: K, value: => Task[Option[V]], establishLock: Boolean = true)
           (f: Forge[Option[V], Option[V]]): Task[Option[V]] = if establishLock then {
    Task(lock(key)).flatMap { _ =>
      Task.defer(value).flatMap(f(_)).guarantee(Task(unlock(key)))
    }
  } else {
    value.flatMap(f(_))
  }

  /** Takes `key`, waiting while another holder has it, then reads `value`. The caller must [[release]] the key. */
  def acquire(key: K, value: => Task[Option[V]]): Task[Option[V]] =
    Task(lock(key)).flatMap(_ => Task.defer(value).handleError(t => Task(unlock(key)).next(Task.error(t))))

  /** Releases `key` and returns `newValue`. Nothing is kept for the next holder, which reads its own value. */
  def release(key: K, newValue: => Option[V]): Task[Option[V]] = Task {
    unlock(key)
    newValue
  }

  /** Takes `key`, blocking while another holder has it. Each call is paired with one [[unlock]]. */
  def lock(key: K): Unit = {
    val entry = locks.compute(key, (_, existing) => {
      val e = if existing == null then new Entry else existing
      e.users += 1
      e
    })
    try entry.semaphore.acquire()
    catch {
      case t: Throwable =>
        leave(key, entry, held = false)
        throw t
    }
  }

  /** Releases `key`, taken by [[lock]]. */
  def unlock(key: K): Unit = {
    val entry = locks.get(key)
    if entry != null then leave(key, entry, held = true)
  }

  /** Whether a thread holds or waits for `key`. */
  def isLocked(key: K): Boolean = locks.containsKey(key)

  private def leave(key: K, entry: Entry, held: Boolean): Unit = {
    locks.compute(key, (_, existing) => {
      if held then entry.semaphore.release()
      if existing eq entry then {
        entry.users -= 1
        if entry.users == 0 then null else entry
      } else existing
    })
    ()
  }
}
