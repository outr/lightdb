package lightdb.sql

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import lightdb.document.Document
import lightdb.transaction.{Transaction, TransactionKey}

import java.sql.Connection

case class HikariConnectionManager[D <: Document[D]](config: SQLConfig) extends ConnectionManager[D] {
  private lazy val connectionKey: TransactionKey[Connection] = TransactionKey("sqlConnection")

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

  override def getConnection(implicit transaction: Transaction[D]): Connection = transaction
    .getOrCreate(connectionKey, openConnection())

  override def currentConnection(implicit transaction: Transaction[D]): Option[Connection] =
    transaction.get(connectionKey)

  override def releaseConnection(implicit transaction: Transaction[D]): Unit =
    currentConnection.foreach(closeConnection)
}
