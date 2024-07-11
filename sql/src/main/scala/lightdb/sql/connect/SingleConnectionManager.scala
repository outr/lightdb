package lightdb.sql.connect

import lightdb.doc.Document
import lightdb.transaction.Transaction

import java.sql.{Connection, DriverManager}

case class SingleConnectionManager[Doc <: Document[Doc]](connection: java.sql.Connection) extends ConnectionManager[Doc] {
  override def getConnection(implicit transaction: Transaction[Doc]): Connection = connection

  override def currentConnection(implicit transaction: Transaction[Doc]): Option[Connection] = Some(connection)

  override def releaseConnection(implicit transaction: Transaction[Doc]): Unit = {}

  override def dispose(): Unit = {
    connection.commit()
    connection.close()
  }
}

object SingleConnectionManager {
  def apply[Doc <: Document[Doc]](config: SQLConfig): SingleConnectionManager[Doc] = {
    val connection = {
      val c = DriverManager.getConnection(config.jdbcUrl, config.username.orNull, config.password.orNull)
      c.setAutoCommit(config.autoCommit)
      c
    }
    SingleConnectionManager(connection)
  }
}