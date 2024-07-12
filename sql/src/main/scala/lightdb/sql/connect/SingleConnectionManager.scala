package lightdb.sql.connect

import lightdb.doc.Document
import lightdb.transaction.Transaction

import java.sql.{Connection, DriverManager}

case class SingleConnectionManager(connection: java.sql.Connection) extends ConnectionManager {
  override def getConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Connection = connection

  override def currentConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Option[Connection] = Some(connection)

  override def releaseConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Unit = {}

  override def dispose(): Unit = {
    connection.commit()
    connection.close()
  }
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