package lightdb

import rapid.Task
import rapid.task.Completable

import java.nio.file.Path

import scala.collection.mutable

/**
 * Scala.js implementations of the few behaviors that differ between the JVM and Scala.js builds of core.
 *
 * JavaScript is single-threaded, but tasks still interleave at every asynchronous boundary (an IndexedDB request,
 * a timer), and nothing may block. `exclusive` therefore serializes by chaining each caller behind the previous one
 * instead of holding a monitor.
 */
private[lightdb] object LightDBPlatform {
  /** There is no filesystem, and no `Path` can exist here (the class is not linked), so stores have no path. */
  def storePath(directory: Option[Path], name: String): Option[Path] = None

  /** Browsers have no hook that can await asynchronous work at unload; writes are durable at each commit instead. */
  def onShutdown(task: => Task[Unit]): Unit = ()

  // Most recent caller per lock. Identity keys: the lock is an arbitrary object, as with `synchronized` on the JVM.
  private val tails = mutable.Map.empty[LockKey, Completable[Unit]]

  private final class LockKey(val lock: AnyRef) {
    override def hashCode(): Int = System.identityHashCode(lock)
    override def equals(obj: Any): Boolean = obj match {
      case k: LockKey => k.lock eq lock
      case _ => false
    }
  }

  /** Run `task` so that no other `exclusive` call on the same `lock` overlaps it. */
  def exclusive[T](lock: AnyRef)(task: => Task[T]): Task[T] = Task.defer {
    val key = new LockKey(lock)
    val done = Task.completable[Unit]
    val previous: Task[Unit] = tails.put(key, done).getOrElse(Task.unit)
    previous.next(Task.defer(task)).guarantee(Task {
      if (tails.get(key).exists(_ eq done)) tails.remove(key)
      done.success(())
    })
  }
}
