package lightdb.util

import rapid._

/**
 * Provides simple initialization support to avoid initialization being invoked more
 * than once. FlatMap on `init` to safely guarantee initialization was successful.
 */
trait Initializable {
  @volatile private var initialized = false

  /**
   * Calls initialize() exactly one time. Safe to call multiple times.
   */
  lazy val init: Task[Unit] = initialize().map { _ =>
    initialized = true
  }.singleton

  def isInitialized: Boolean = initialized

  /**
   * Define initialization functionality here, but never call directly.
   */
  protected def initialize(): Task[Unit]
}