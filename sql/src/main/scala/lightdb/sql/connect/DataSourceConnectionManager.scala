package lightdb.sql.connect

import lightdb.doc.Document
import lightdb.sql._
import lightdb.transaction.Transaction

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

  override def getConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Connection = {
    synchronized {
      if (transaction.connection == null) {
        transaction.connection = openConnection()
      }
    }
    transaction.connection
  }

  override def currentConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Option[Connection] =
    Option(transaction.connection)

  override def releaseConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Unit =
    currentConnection.foreach(closeConnection)
}