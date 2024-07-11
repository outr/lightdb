package lightdb.sql

import lightdb.doc.Document
import lightdb.sql.connect.ConnectionManager
import lightdb.transaction.Transaction

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

case class SQLTransaction[Doc <: Document[Doc]](connectionManager: ConnectionManager[Doc], store: SQLStore[Doc, _]) extends Transaction[Doc] {
  private var ps: PreparedStatement = _
  private[sql] var connection: Connection = _
  private[sql] val batch = new AtomicInteger(0)
  private var statements = List.empty[Statement]
  private var resultSets = List.empty[ResultSet]

  private var cache = Map.empty[String, PreparedStatement]

  def withPreparedStatement[Return](sql: String, cache: Boolean)(f: PreparedStatement => Return): Return = {
    val connection = connectionManager.getConnection(this)
    def createPs() = {
      val ps = connection.prepareStatement(sql)
      register(ps)
      ps
    }
    if (cache) {
      val ps = synchronized {
        this.cache.get(sql) match {
          case Some(ps) => ps
          case None =>
            val ps = createPs()
            this.cache += sql -> ps
            ps
        }
      }
      ps.synchronized {
        f(ps)
      }
    } else {
      f(createPs())
    }
  }

  def withPreparedStatement[Return](f: PreparedStatement => Return): Return = synchronized {
    if (ps == null) {
      val connection = connectionManager.getConnection(this)
      ps = connection.prepareStatement(store.insertSQL)
    }
    f(ps)
  }

  def register(s: Statement): Unit = synchronized {
    statements = (s :: statements).distinct
  }

  def register(rs: ResultSet): Unit = synchronized {
    resultSets = rs :: resultSets
  }

  override def commit(): Unit = if (connection != null) {
    connection.commit()
  }

  override def rollback(): Unit = if (connection != null) {
    connection.rollback()
  }

  def close(): Unit = {
    if (batch.get() > 0) {
      ps.executeBatch()
    }
    resultSets.foreach(rs => Try(rs.close()))
    statements.foreach(s => Try(s.close()))
    if (ps != null) ps.close()
    connectionManager.releaseConnection(this)
  }
}
