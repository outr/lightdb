package lightdb.sql.connect

import lightdb.Transaction

import java.sql.Connection

trait ConnectionManager[Doc] {
  def getConnection(implicit transaction: Transaction[Doc]): Connection

  def currentConnection(implicit transaction: Transaction[Doc]): Option[Connection]

  def releaseConnection(implicit transaction: Transaction[Doc]): Unit

  def dispose(): Unit
}
