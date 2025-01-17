package lightdb.sql.connect

import lightdb.doc.Document
import lightdb.transaction.Transaction
import rapid.Task

import java.sql.{Connection, DriverManager}

case class SingleConnectionManager(connection: java.sql.Connection) extends ConnectionManager {
  override def getConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Connection = connection

  override def currentConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Option[Connection] = Some(connection)

  override def releaseConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Unit = {}

  override protected def doDispose(): Task[Unit] = Task {
    if (!connection.getAutoCommit) connection.commit()
    connection.close()
  }.when(!connection.isClosed)
}

object SingleConnectionManager {
  def apply(config: SQLConfig): SingleConnectionManager = {
    val connection = {
      val c = DriverManager.getConnection(config.jdbcUrl, config.username.orNull, config.password.orNull)
      c.setAutoCommit(config.autoCommit)
      c
    }
    SingleConnectionManager(connection)
  }
}