package lightdb.util

import rapid.Task

/**
 * Provides simple disposal support to avoid dispose being invoked more than one. FlatMap on `dispose`
 * to safely guarantee disposal was successful.
 */
trait Disposable {
  @volatile private var disposing = false
  @volatile private var disposed = false

  /**
   * Calls doDispose() exactly one time. Safe to call multiple times.
   */
  lazy val dispose: Task[Unit] = Task { disposing = true }.next(doDispose()).map { _ =>
    disposed = true
  }.singleton

  def isDisposes: Boolean = disposed

  /** True once disposal has started, including while it is still running. */
  def isDisposing: Boolean = disposing

  protected def doDispose(): Task[Unit]
}
