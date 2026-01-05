package lightdb.sql

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import lightdb.store.Store
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
      val ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
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

  def returnPreparedStatement(sql: String, ps: PreparedStatement): Unit = {
    if (ps == null) return
    if (caching) {
      cache.computeIfAbsent(sql, _ => new ConcurrentLinkedQueue[PreparedStatement]).add(ps)
    } else {
      Try(ps.close())
    }
  }

  /**
   * Closes open result sets and non-prepared statements for this state.
   * Needed for drivers like DuckDB that disallow executing new statements
   * while a previous result set is still open. Prepared statements are
   * left alone to avoid breaking statement pooling/reuse.
   */
  def closePendingResults(): Unit = synchronized {
    // Ensure read-your-writes semantics: SQLStoreTransaction buffers inserts/upserts using JDBC batches.
    // Before executing any new statements (especially SELECTs), flush pending batches so subsequent reads
    // within the same transaction can see the writes.
    flushBatches()

    resultSets.foreach(rs => Try(rs.close()))
    resultSets = Nil
    val (prepared, regular) = statements.partition(_.isInstanceOf[java.sql.PreparedStatement])
    regular.foreach(s => Try(s.close()))
    statements = prepared
  }

  private def flushBatches(): Unit = synchronized {
    if (batchInsert.get() > 0 && psInsert != null) {
      psInsert.executeBatch()
      batchInsert.set(0)
    }
    if (batchUpsert.get() > 0 && psUpsert != null) {
      psUpsert.executeBatch()
      batchUpsert.set(0)
    }
  }

  def markDirty(): Unit = dirty = true

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
    // Only track non-prepared statements; prepared statements may be pooled.
    s match {
      case _: PreparedStatement => ()
      case _ => statements = (s :: statements).distinct
    }
  }

  def register(rs: ResultSet): Unit = synchronized {
    resultSets = rs :: resultSets
  }

  def commit: Task[Unit] = Task {
    if (dirty) {
      // TODO: SingleConnection shares
      if (batchInsert.get() > 0) {
        psInsert.executeBatch()
        batchInsert.set(0)
      }
      if (batchUpsert.get() > 0) {
        psUpsert.executeBatch()
        batchUpsert.set(0)
      }
      dirty = false
      commitInternal()
    }
  }

  private def commitInternal(): Unit = {
    Try(connectionManager.getConnection(this).commit()).failed.foreach { t =>
      scribe.warn(s"Commit failed: ${t.getMessage}")
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
      batchInsert.set(0)
    }
    if (batchUpsert.get() > 0) {
      psUpsert.executeBatch()
      batchUpsert.set(0)
    }
    resultSets.foreach(rs => Try(rs.close()))
    statements.foreach(s => Try(s.close()))
    if (psInsert != null) psInsert.close()
    if (psUpsert != null) psUpsert.close()
    connectionManager.releaseConnection(this)
  }
}