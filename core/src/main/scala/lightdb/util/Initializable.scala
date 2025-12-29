package lightdb.util

import rapid._

/**
 * Provides simple initialization support to avoid initialization being invoked more
 * than once. FlatMap on `init` to safely guarantee initialization was successful.
 */
trait Initializable {
  @volatile private var initialized = false

  /**
   * Hook invoked once, immediately before initialization begins.
   *
   * Useful for enforcing invariants like "init has started" without having to override `init`.
   */
  protected def beforeInitialize(): Unit = ()

  /**
   * Calls initialize() exactly one time. Safe to call multiple times.
   */
  lazy val init: Task[Unit] = Task {
    beforeInitialize()
  }.next(initialize()).map { _ =>
    initialized = true
  }.singleton

  def isInitialized: Boolean = initialized

  /**
   * Define initialization functionality here, but never call directly.
   */
  protected def initialize(): Task[Unit]
}