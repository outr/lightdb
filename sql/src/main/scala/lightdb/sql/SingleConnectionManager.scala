package lightdb.sql

import lightdb.document.Document
import lightdb.transaction.Transaction

import java.sql.{Connection, DriverManager}

case class SingleConnectionManager[D <: Document[D]](config: SQLConfig) extends ConnectionManager[D] {
  private lazy val connection = {
    val c = DriverManager.getConnection(config.jdbcUrl, config.username.orNull, config.password.orNull)
    c.setAutoCommit(false)
    c
  }

  override def getConnection(implicit transaction: Transaction[D]): Connection = connection

  override def currentConnection(implicit transaction: Transaction[D]): Option[Connection] = Some(connection)

  override def releaseConnection(implicit transaction: Transaction[D]): Unit = {}
}
