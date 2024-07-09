package lightdb.sql.connect

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import lightdb.sql._
import lightdb.transaction.Transaction

import java.sql.Connection

case class HikariConnectionManager[Doc](config: SQLConfig) extends ConnectionManager[Doc] {
  private lazy val dataSource: HikariDataSource = {
    val hc = new HikariConfig
    hc.setJdbcUrl(config.jdbcUrl)
    config.username.foreach(hc.setUsername)
    config.password.foreach(hc.setPassword)
    config.maximumPoolSize.foreach(hc.setMaximumPoolSize)
    hc.setAutoCommit(config.autoCommit)
    new HikariDataSource(hc)
  }

  private def openConnection(): Connection = {
    val c = dataSource.getConnection
    c
  }

  private def closeConnection(connection: Connection): Unit = {
    connection.close()
  }

  override def getConnection(implicit transaction: Transaction[Doc]): Connection = {
    synchronized {
      if (transaction.connection == null) {
        transaction.connection = openConnection()
      }
    }
    transaction.connection
  }

  override def currentConnection(implicit transaction: Transaction[Doc]): Option[Connection] =
    Option(transaction.connection)

  override def releaseConnection(implicit transaction: Transaction[Doc]): Unit =
    currentConnection.foreach(closeConnection)

  override def dispose(): Unit = dataSource.close()
}
