package lightdb.util

import cats.effect.IO

/**
 * Provides simple initialization support to avoid initialization being invoked more
 * than once. FlatMap on `init` to safely guarantee initialization was successful.
 */
trait Initializable {
  /**
   * Calls initialize() exactly one time. Safe to call multiple times.
   */
  lazy val init: IO[Unit] = LazyIO(initialize())

  /**
   * Define initialization functionality here, but never call directly.
   */
  protected def initialize(): IO[Unit]
}