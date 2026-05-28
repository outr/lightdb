package lightdb.sql.connect

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.*

import java.sql.Connection
import javax.sql.DataSource

trait DataSourceConnectionManager extends ConnectionManager {
  protected def dataSource: DataSource

  private def openConnection(): Connection = {
    val c = dataSource.getConnection
    c
  }

  private def closeConnection(connection: Connection): Unit = {
    connection.commit()
    connection.close()
  }

  override def getConnection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Connection = {
    // Intentionally NOT `synchronized`. `state` is per-transaction and SQL
    // transactions are single-threaded (maximumConcurrency = 1), so the lazy
    // init races with nobody. A monitor here was also a manager-wide lock that
    // serialized every connection acquisition globally AND, because it wrapped
    // the blocking `openConnection()`, pinned virtual-thread carriers on
    // JDK < 24 — exhausting the scheduler under concurrent load.
    if state.connection == null then {
      state.connection = openConnection()
    }
    state.connection
  }

  override def currentConnection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Option[Connection] = {
    Option(state.connection)
  }

  override def releaseConnection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Unit = {
    currentConnection(state).foreach(closeConnection)
  }
}