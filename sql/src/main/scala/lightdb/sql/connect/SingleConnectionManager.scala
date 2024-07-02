package lightdb.sql.connect

import lightdb.Transaction

import java.sql.{Connection, DriverManager}

case class SingleConnectionManager[Doc](config: SQLConfig) extends ConnectionManager[Doc] {
  private lazy val connection = {
    val c = DriverManager.getConnection(config.jdbcUrl, config.username.orNull, config.password.orNull)
    c.setAutoCommit(config.autoCommit)
    c
  }

  override def getConnection(implicit transaction: Transaction[Doc]): Connection = connection

  override def currentConnection(implicit transaction: Transaction[Doc]): Option[Connection] = Some(connection)

  override def releaseConnection(implicit transaction: Transaction[Doc]): Unit = {}
}
