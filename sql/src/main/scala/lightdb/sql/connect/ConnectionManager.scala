package lightdb.sql.connect

import lightdb.doc.Document
import lightdb.transaction.Transaction
import lightdb.util.Disposable
import rapid.Task

import java.sql.Connection

trait ConnectionManager extends Disposable {
  def getConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Connection

  def currentConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Option[Connection]

  def releaseConnection[Doc <: Document[Doc]](implicit transaction: Transaction[Doc]): Unit

  def dispose(): Task[Unit]
}