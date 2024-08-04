package lightdb.sql

import lightdb.doc.Document
import lightdb.sql.connect.ConnectionManager
import lightdb.transaction.{Transaction, TransactionFeature, TransactionKey}

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Try

case class SQLState[Doc <: Document[Doc]](connectionManager: ConnectionManager,
                                          transaction: Transaction[Doc],
                                          store: SQLStore[Doc, _],
                                          caching: Boolean) extends TransactionFeature {
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
    val connection = connectionManager.getConnection(transaction)

    def createPs(): PreparedStatement = {
      val ps = connection.prepareStatement(sql)
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
      val connection = connectionManager.getConnection(transaction)
      psInsert = connection.prepareStatement(store.insertSQL)
    }
    dirty = true
    f(psInsert)
  }

  def withUpsertPreparedStatement[Return](f: PreparedStatement => Return): Return = synchronized {
    if (psUpsert == null) {
      val connection = connectionManager.getConnection(transaction)
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

  override def commit(): Unit = if (dirty) {
    // TODO: SingleConnection shares
    dirty = false
    Try(connectionManager.getConnection(transaction).commit()).failed.foreach { t =>
      scribe.warn(s"Commit failed: ${t.getMessage}")
    }
  }

  override def rollback(): Unit = if (dirty) {
    dirty = false
    Try(connectionManager.getConnection(transaction).rollback()).failed.foreach { t =>
      scribe.warn(s"Rollback failed: ${t.getMessage}")
    }
  }

  override def close(): Unit = {
    super.close()
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
    connectionManager.releaseConnection(transaction)
  }
}