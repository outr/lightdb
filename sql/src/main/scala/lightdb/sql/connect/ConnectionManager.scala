package lightdb.sql.connect

import lightdb.doc.Document
import lightdb.transaction.Transaction
import lightdb.util.Disposable
import rapid.Task

import java.sql.Connection
import java.util.concurrent.atomic.AtomicInteger

trait ConnectionManager extends Disposable {
  private val using = new AtomicInteger(0)

  def active(): Unit = using.incrementAndGet()

  def release(): Task[Unit] = {
    val current = using.decrementAndGet()
    if (current <= 0) {
      doDispose()
    } else {
      Task.unit
    }
  }

  def getConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Connection

  def currentConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Option[Connection]

  def releaseConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Unit
}