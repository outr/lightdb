package lightdb.sql.connect

import lightdb.doc.Document
import lightdb.transaction.Transaction

import java.sql.Connection

trait ConnectionManager[Doc <: Document[Doc]] {
  def getConnection(implicit transaction: Transaction[Doc]): Connection

  def currentConnection(implicit transaction: Transaction[Doc]): Option[Connection]

  def releaseConnection(implicit transaction: Transaction[Doc]): Unit

  def dispose(): Unit
}
