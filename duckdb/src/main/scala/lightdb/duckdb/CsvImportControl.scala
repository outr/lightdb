package lightdb.duckdb

import java.util.concurrent.atomic.AtomicReference

/**
 * Lets another thread stop an import that is running (`Statement.cancel`): the import then fails and its transaction is rolled back,
 * so no half-loaded table is left. One control serves one import.
 */
final class CsvImportControl {
  private val running = new AtomicReference[java.sql.Statement](null)
  @volatile private var cancelled = false

  def isCancelled: Boolean = cancelled

  def cancel(): Unit = {
    cancelled = true
    Option(running.get()).foreach(s => try s.cancel() catch { case _: Throwable => () })
  }

  private[duckdb] def running(statement: java.sql.Statement): Unit = {
    running.set(statement)
    if cancelled then try statement.cancel() catch { case _: Throwable => () }
  }
}
