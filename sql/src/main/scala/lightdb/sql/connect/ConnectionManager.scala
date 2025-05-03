package lightdb.sql.connect

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.SQLState
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

  def getConnection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Connection

  def currentConnection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Option[Connection]

  def releaseConnection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Unit
}