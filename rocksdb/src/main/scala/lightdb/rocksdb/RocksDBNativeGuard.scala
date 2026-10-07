package lightdb.rocksdb

import fabric.rw.intRW
import lightdb.error.StoreDisposedException
import profig.Profig

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Guards one native RocksDB instance against use after close. RocksJava does not clear its native handles when the
 * database closes, so a call that reaches it afterwards dereferences freed memory and crashes the JVM. Every native
 * call runs inside [[apply]], which counts it in flight and refuses it with a [[StoreDisposedException]] once the
 * guard is closed; [[close]] refuses new calls and waits for those in flight to finish before the owner closes the
 * database.
 */
final class RocksDBNativeGuard {
  private val closed = new AtomicBoolean(false)
  private val inFlight = new AtomicInteger(0)
  private val monitor = new Object

  def isClosed: Boolean = closed.get()

  /** Runs `f` as a native call, or refuses it for `storeName` when the guard is closed. */
  def apply[R](storeName: String)(f: => R): R = {
    inFlight.incrementAndGet()
    try {
      if closed.get() then throw StoreDisposedException(storeName)
      f
    } finally {
      if inFlight.decrementAndGet() == 0 && closed.get() then monitor.synchronized(monitor.notifyAll())
    }
  }

  /** Runs `f` as a native call if the guard is still open. */
  def ifOpen[R](f: => R): Option[R] = {
    inFlight.incrementAndGet()
    try {
      if closed.get() then None else Some(f)
    } finally {
      if inFlight.decrementAndGet() == 0 && closed.get() then monitor.synchronized(monitor.notifyAll())
    }
  }

  /**
   * Refuses every later call and waits up to `timeout` for the calls in flight to finish. Returns true when none
   * remain, so the database can be closed safely.
   */
  def close(timeout: FiniteDuration): Boolean = {
    closed.set(true)
    val deadline = System.nanoTime() + timeout.toNanos
    monitor.synchronized {
      while inFlight.get() > 0 && System.nanoTime() < deadline do {
        monitor.wait(math.max(1L, math.min(100L, (deadline - System.nanoTime()) / 1_000_000L)))
      }
    }
    inFlight.get() == 0
  }

  def inFlightCount: Int = inFlight.get()
}

object RocksDBNativeGuard {
  /** How long disposal waits for native calls in flight before giving up on closing the database. */
  def disposeTimeout: FiniteDuration = Profig("lightdb.rocksdb.disposeTimeoutSeconds").opt[Int].getOrElse(30).seconds
}
