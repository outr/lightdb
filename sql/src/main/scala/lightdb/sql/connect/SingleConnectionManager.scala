package lightdb.sql.connect

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.SQLState
import lightdb.transaction.Transaction
import rapid.Task

import java.sql.{Connection, DriverManager}

case class SingleConnectionManager(connectionCreator: () => java.sql.Connection) extends ConnectionManager {
  private var _connection: java.sql.Connection = _
  private def connection: java.sql.Connection = {
    if (_connection == null || _connection.isClosed) {
      _connection = connectionCreator()
    }
    _connection
  }

  override def getConnection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Connection = connection

  override def currentConnection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Option[Connection] = Some(connection)

  override def releaseConnection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](state: SQLState[Doc, Model]): Unit = ()

  override protected def doDispose(): Task[Unit] = Task {
    if (!connection.getAutoCommit) connection.commit()
    connection.close()
  }.when(!connection.isClosed)
}

object SingleConnectionManager {
  def apply(config: SQLConfig): SingleConnectionManager = {
    SingleConnectionManager(() => {
      val c = DriverManager.getConnection(config.jdbcUrl, config.username.orNull, config.password.orNull)
      c.setAutoCommit(config.autoCommit)
      c
    })
  }
}