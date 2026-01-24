package lightdb.rocksdb

import rapid.{SingleThreadAgent, Task, Unique}
import rapid.taskSeq2Ops

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A small pool of long-lived single-thread agents that can be leased to iterator wrappers.
 *
 * Why: RocksDB iterators are thread-confined. Previously we created a brand new SingleThreadAgent (and OS thread)
 * per iterator. Under heavy prefix probing this can explode thread count. This pool reuses a bounded set of threads
 * while still keeping each iterator confined to a single thread for its entire lifetime (lease).
 */
final class RocksDBIteratorThreadPool(namePrefix: String, size: Int) {
  require(size > 0, s"iterator thread pool size must be > 0, but was $size")

  private val q = new LinkedBlockingQueue[SingleThreadAgent[Unit]](size)
  private val disposed = new AtomicBoolean(false)

  private val all: Array[SingleThreadAgent[Unit]] = (0 until size).map { i =>
    val agent = SingleThreadAgent[Unit](s"$namePrefix-itp-$i")(Task.unit)
    q.put(agent)
    agent
  }.toArray

  /**
   * Acquire a lease. This will try to use the bounded pool, but will not block forever:
   * if all pool threads are busy for too long, we create a temporary agent.
   *
   * This avoids deadlocks/hangs if some iterator isn't closed promptly.
   */
  def acquire(waitSeconds: Int = 5): SingleThreadAgent[Unit] = {
    if disposed.get() then throw new IllegalStateException(s"$namePrefix iterator thread pool is disposed")
    val a = q.poll(waitSeconds.toLong, TimeUnit.SECONDS)
    if a != null then a
    else {
      // Temporary escape hatch: avoid deadlock at the cost of extra threads.
      SingleThreadAgent[Unit](s"$namePrefix-itp-extra-${Unique.withLength(6)()}")(Task.unit)
    }
  }

  def release(agent: SingleThreadAgent[Unit]): Unit = {
    // Return pool agents; dispose temporary agents.
    val isPooled = all.contains(agent)
    if isPooled && !disposed.get() then {
      q.put(agent)
    } else {
      agent.dispose().attempt.unit.sync()
    }
  }

  def dispose(): Task[Unit] = Task.defer {
    if disposed.compareAndSet(false, true) then {
      // Dispose all known pool agents without blocking on leases returning.
      all.toList.map(_.dispose().attempt.unit).tasks.unit
    } else {
      Task.unit
    }
  }
}

