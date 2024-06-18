package lightdb.util

import cats.effect.IO

import java.util.concurrent.atomic.AtomicInteger

/**
 * Provides simple initialization support to avoid initialization being invoked more
 * than once. FlatMap on `init` to safely guarantee initialization was successful.
 */
trait Initializable {
  private val status = new AtomicInteger(0)

  def isInitialized: Boolean = status.get() == 2

  /**
   * Calls initialize() exactly one time. Safe to call multiple times.
   */
  final def init(): IO[Boolean] = if (status.compareAndSet(0, 1)) {
    initialize().map { _ =>
      status.set(2)
      true
    }
  } else {
    IO.pure(false)
  }

  /**
   * Define initialization functionality here, but never call directly.
   */
  protected def initialize(): IO[Unit]
}