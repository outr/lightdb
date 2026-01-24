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
    synchronized {
      if state.connection == null then {
        state.connection = openConnection()
      }
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