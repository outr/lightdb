package lightdb.sql

import lightdb.doc.Document
import lightdb.sql.connect.ConnectionManager
import lightdb.transaction.Transaction

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

case class SQLTransaction[Doc <: Document[Doc]](connectionManager: ConnectionManager,
                                                store: SQLStore[Doc, _],
                                                caching: Boolean) extends Transaction[Doc] {
  private var psInsert: PreparedStatement = _
  private var psUpsert: PreparedStatement = _
  private[sql] var connection: Connection = _
  private[sql] val batchInsert = new AtomicInteger(0)
  private[sql] val batchUpsert = new AtomicInteger(0)
  private var statements = List.empty[Statement]
  private var resultSets = List.empty[ResultSet]

  private lazy val cache = new ConcurrentHashMap[String, ConcurrentLinkedQueue[PreparedStatement]]

  def withPreparedStatement[Return](sql: String)(f: PreparedStatement => Return): Return = {
    val connection = connectionManager.getConnection(this)
    def createPs(): PreparedStatement = {
      val ps = connection.prepareStatement(sql)
      register(ps)
      ps
    }
    if (caching) {
      val ps = synchronized {
        val q = Option(this.cache.get(sql)) match {
          case Some(q) => q
          case None =>
            val q = new ConcurrentLinkedQueue[PreparedStatement]
            cache.put(sql, q)
            q
        }
        Option(q.poll()) match {
          case Some(ps) => ps
          case None => createPs()
        }
      }
      ps.synchronized {
        f(ps)
      }
    } else {
      f(createPs())
    }
  }

  def returnPreparedStatement(sql: String, ps: PreparedStatement): Unit = if (caching) {
    cache.get(sql).add(ps)
  }

  def withInsertPreparedStatement[Return](f: PreparedStatement => Return): Return = synchronized {
    if (psInsert == null) {
      val connection = connectionManager.getConnection(this)
      psInsert = connection.prepareStatement(store.insertSQL)
    }
    f(psInsert)
  }

  def withUpsertPreparedStatement[Return](f: PreparedStatement => Return): Return = synchronized {
    if (psUpsert == null) {
      val connection = connectionManager.getConnection(this)
      psUpsert = connection.prepareStatement(store.upsertSQL)
    }
    f(psUpsert)
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
    if (batchInsert.get() > 0) {
      psInsert.executeBatch()
    }
    if (batchUpsert.get() > 0) {
      psUpsert.executeBatch()
    }
    resultSets.foreach(rs => Try(rs.close()))
    statements.foreach(s => Try(s.close()))
    if (psInsert != null) psInsert.close()
    if (psUpsert != null) psUpsert.close()
    connectionManager.releaseConnection(this)
  }
}