package lightdb.util

import rapid.Task

/**
 * Provides simple disposal support to avoid dispose being invoked more than one. FlatMap on `dispose`
 * to safely guarantee disposal was successful.
 */
trait Disposable {
  @volatile private var disposed = false

  /**
   * Calls doDispose() exactly one time. Safe to call multiple times.
   */
  lazy val dispose: Task[Unit] = doDispose().map { _ =>
    disposed = true
  }.singleton

  def isDisposes: Boolean = disposed

  protected def doDispose(): Task[Unit]
}
