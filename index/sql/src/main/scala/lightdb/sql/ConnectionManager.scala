package lightdb.sql

import lightdb.document.Document
import lightdb.transaction.Transaction

import java.sql.Connection

trait ConnectionManager[D <: Document[D]] {
  def getConnection(implicit transaction: Transaction[D]): Connection

  def currentConnection(implicit transaction: Transaction[D]): Option[Connection]

  def releaseConnection(implicit transaction: Transaction[D]): Unit
}
