package lightdb.sql

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import rapid.Task

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.util.Try

case class SQLState[Doc <: Document[Doc], Model <: DocumentModel[Doc]](connectionManager: ConnectionManager,
                                                                       store: SQLStore[Doc, Model],
                                                                       caching: Boolean) {
  private var psInsert: PreparedStatement = _
  private var psUpsert: PreparedStatement = _
  private[sql] var connection: Connection = _
  private[sql] val batchInsert = new AtomicInteger(0)
  private[sql] val batchUpsert = new AtomicInteger(0)
  private var statements = List.empty[Statement]
  private var resultSets = List.empty[ResultSet]
  private var dirty = false

  private lazy val cache = new ConcurrentHashMap[String, ConcurrentLinkedQueue[PreparedStatement]]

  def withPreparedStatement[Return](sql: String)(f: PreparedStatement => Return): Return = {
    val connection = connectionManager.getConnection(this)

    def createPs(): PreparedStatement = {
      val ps = connection.prepareStatement(sql)
      ps.setFetchSize(SQLStoreTransaction.FetchSize)
      register(ps)
      ps
    }

    if (caching) {
      val q = cache.computeIfAbsent(sql, _ => new ConcurrentLinkedQueue[PreparedStatement])
      val ps = Option(q.poll()) match {
        case Some(ps) => ps
        case None => createPs()
      }

      f(ps)
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
    dirty = true
    f(psInsert)
  }

  def withUpsertPreparedStatement[Return](f: PreparedStatement => Return): Return = synchronized {
    if (psUpsert == null) {
      val connection = connectionManager.getConnection(this)
      psUpsert = connection.prepareStatement(store.upsertSQL)
    }
    dirty = true
    f(psUpsert)
  }

  def register(s: Statement): Unit = synchronized {
    statements = (s :: statements).distinct
  }

  def register(rs: ResultSet): Unit = synchronized {
    resultSets = rs :: resultSets
  }

  def commit: Task[Unit] = Task {
    if (dirty) {
      // TODO: SingleConnection shares
      if (batchInsert.get() > 0) {
        psInsert.executeBatch()
      }
      if (batchUpsert.get() > 0) {
        psUpsert.executeBatch()
      }
      dirty = false
      Try(connectionManager.getConnection(this).commit()).failed.foreach { t =>
        scribe.warn(s"Commit failed: ${t.getMessage}")
      }
    }
  }

  def rollback: Task[Unit] = Task {
    if (dirty) {
      dirty = false
      Try(connectionManager.getConnection(this).rollback()).failed.foreach { t =>
        scribe.warn(s"Rollback failed: ${t.getMessage}")
      }
    }
  }

  def close: Task[Unit] = Task {
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