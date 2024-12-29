package lightdb.util

import rapid._

/**
 * Provides simple initialization support to avoid initialization being invoked more
 * than once. FlatMap on `init` to safely guarantee initialization was successful.
 */
trait Initializable {
  @volatile private var initialized = false
  private lazy val singleton = initialize().map { _ =>
    initialized = true
  }.singleton

  def isInitialized: Boolean = initialized

  /**
   * Calls initialize() exactly one time. Safe to call multiple times.
   */
  final def init(): Task[Unit] = singleton

  /**
   * Define initialization functionality here, but never call directly.
   */
  protected def initialize(): Task[Unit]
}