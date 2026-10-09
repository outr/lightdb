package lightdb.sql

import lightdb.doc.{Document, DocumentModel}
import lightdb.sql.connect.ConnectionManager
import lightdb.store.Store
import rapid.Task

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.util.Try

case class SQLState[Doc <: Document[Doc], Model <: DocumentModel[Doc]](connectionManager: ConnectionManager,
                                                                       store: SQLStore[Doc, Model],
                                                                       caching: Boolean) {
  private var psInsert: PreparedStatement = _
  private var psUpsert: PreparedStatement = _
  // `@volatile`: a transaction's sequential tasks may hop carrier threads, and
  // the connection is now published without a lock (see DataSourceConnectionManager).
  @volatile private[sql] var connection: Connection = _
  private[sql] val batchInsert = new AtomicInteger(0)
  private[sql] val batchUpsert = new AtomicInteger(0)
  private var statements = List.empty[Statement]
  private var resultSets = List.empty[ResultSet]
  private var dirty = false
  @volatile private var released = false
  private val stateLock = new ReentrantLock()

  /** Whether [[close]] has run. A released state never opens a connection again. */
  def isReleased: Boolean = released

  /** Fail a use of this state after [[close]]: it would open a connection nothing ever commits or closes. */
  private[sql] def ensureOpen(): Unit =
    if released then throw new IllegalStateException(s"transaction for ${Option(store).map(_.name).getOrElse("<unknown store>")} used after release")

  // JDBC calls and pool checkout can block. Holding an intrinsic monitor here pins virtual-thread
  // carriers on Java 21, starving the borrowers that need to resume and return pool connections.
  private def withStateLock[A](f: => A): A = {
    stateLock.lock()
    try f finally stateLock.unlock()
  }

  private lazy val cache = new ConcurrentHashMap[String, ConcurrentLinkedQueue[PreparedStatement]]

  // -- Joined transactions --------------------------------------------------------------------------------------------
  // A joined state borrows its owner's connection: it writes on it, never commits, rolls back or releases it, and never
  // replaces it. The owner's COMMIT or rollback decides both.
  @volatile private var joinedTo: Option[SQLState[?, ?]] = None

  /** Whether this state borrows another transaction's connection. */
  def isJoined: Boolean = joinedTo.nonEmpty

  private[sql] def joinTo(owner: SQLState[?, ?]): Unit = {
    if owner.connectionManager ne connectionManager then
      throw new IllegalArgumentException("a joined SQL transaction must share its owner's connection manager")
    joinedTo = Some(owner)
  }

  /** The connection this state writes on: its own (opened on first use), or its owner's when joined. */
  def openConnection(): java.sql.Connection = joinedTo match {
    case Some(owner) =>
      ensureOpen()
      owner.openConnection()
    case None => connectionManager.getConnection(this)
  }


  private def dirtied(): Unit = {
    dirty = true
    joinedTo.foreach(_.markDirty())
  }

  /** A joined state's release: its queued batches reach the owner's connection, uncommitted. */
  private[sql] def stage(): Unit = withStateLock {
    ensureOpen()
    flushBatches()
  }

  def withPreparedStatement[Return](sql: String)(f: PreparedStatement => Return): Return = withStateLock {
    ensureOpen()
    val connection = openConnection()

    def createPs(): PreparedStatement = {
      val ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      ps.setFetchSize(SQLStoreTransaction.FetchSize)
      register(ps)
      ps
    }

    if caching then {
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

  def returnPreparedStatement(sql: String, ps: PreparedStatement): Unit = withStateLock {
    if ps == null then ()
    else if caching then {
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
  def closePendingResults(): Unit = withStateLock {
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

  private def flushBatches(): Unit = withStateLock {
    if batchInsert.get() > 0 && psInsert != null then {
      psInsert.executeBatch()
      batchInsert.set(0)
    }
    if batchUpsert.get() > 0 && psUpsert != null then {
      psUpsert.executeBatch()
      batchUpsert.set(0)
    }
  }

  def markDirty(): Unit = withStateLock {
    dirtied()
  }

  /**
   * Whether a failed read may be retried on a fresh connection.
   *
   * Only when this transaction has no uncommitted writes. Those writes live on the very connection a
   * retry throws away, so retrying around them would silently drop them and report success.
   */
  private[sql] def retryableRead: Boolean = withStateLock {
    joinedTo.isEmpty && !dirty && batchInsert.get() == 0 && batchUpsert.get() == 0
  }

  /**
   * Throw away this transaction's connection and everything bound to it, so the next call opens a
   * fresh one. For use when the connection is already known dead (see the stale-plan retry in
   * [[SQLStoreTransaction.resultsFor]]).
   *
   * The cached prepared statements have to go with it: each belongs to that connection, so reusing
   * one afterwards only fails again against something closed. Nothing is closed individually here --
   * the connection is broken, and the pool discards it on release rather than handing it back.
   */
  private[sql] def discardConnection(): Unit = withStateLock {
    require(joinedTo.isEmpty, "a joined SQL transaction never replaces its owner's connection")
    cache.clear()
    statements = Nil
    resultSets = Nil
    psInsert = null
    psUpsert = null
    Try(connectionManager.releaseConnection(this))
    connection = null
  }

  def withInsertPreparedStatement[Return](f: PreparedStatement => Return): Return = withStateLock {
      ensureOpen()
      if psInsert == null then {
        val connection = openConnection()
        psInsert = connection.prepareStatement(store.insertSQL)
      }
      dirtied()
      f(psInsert)
  }

  def withUpsertPreparedStatement[Return](f: PreparedStatement => Return): Return = withStateLock {
      ensureOpen()
      if psUpsert == null then {
        val connection = openConnection()
        psUpsert = connection.prepareStatement(store.upsertSQL)
      }
      dirtied()
      f(psUpsert)
  }

  def register(s: Statement): Unit = withStateLock {
    // Only track non-prepared statements; prepared statements may be pooled.
    s match {
      case _: PreparedStatement => ()
      case _ => statements = (s :: statements).distinct
    }
  }

  def register(rs: ResultSet): Unit = withStateLock {
    resultSets = rs :: resultSets
  }

  /** Commit only here, never at resource release; JDBC failures must reach the caller. */
  def commit: Task[Unit] = Task { withStateLock {
    flushBatches()
    // Also commit raw JDBC/DDL and read transactions, which may not have called markDirty.
    if joinedTo.isEmpty then connectionManager.currentConnection(this).foreach { c =>
      if (!c.getAutoCommit) c.commit()
    }
    dirty = false
  }}

  private def discardBatches(): Unit = {
    batchInsert.set(0)
    batchUpsert.set(0)
    // Discard counters first: even a broken statement must not be re-flushed during cleanup.
    var failure: Throwable = null
    List(psInsert, psUpsert).filter(_ != null).foreach { ps =>
      try ps.clearBatch() catch { case scala.util.control.NonFatal(t) =>
        if (failure == null) failure = t else if (failure ne t) failure.addSuppressed(t)
      }
    }
    if (failure != null) throw failure
  }

  def rollback: Task[Unit] = Task { withStateLock {
    try discardBatches()
    finally {
      dirty = false
      if joinedTo.isEmpty then connectionManager.currentConnection(this).foreach { c =>
        if (!c.isClosed && !c.getAutoCommit) c.rollback()
      }
    }
  }}

  /** Cleanup never flushes. Attempt every close even if one resource is already broken. */
  def close: Task[Unit] = Task { withStateLock {
    var failure: Throwable = null
    def cleanup(f: => Unit): Unit = try f catch { case scala.util.control.NonFatal(t) =>
      if (failure == null) failure = t else if (failure ne t) failure.addSuppressed(t)
    }
    cleanup(discardBatches())
    // Manual close without commit must not leave a dirty single/shared connection to publish later.
    if (dirty && joinedTo.isEmpty) cleanup(connectionManager.currentConnection(this).foreach { c =>
      if (!c.isClosed && !c.getAutoCommit) c.rollback()
    })
    dirty = false
    resultSets.foreach(rs => cleanup(rs.close()))
    resultSets = Nil
    import scala.jdk.CollectionConverters.*
    val prepared = cache.values().asScala.flatMap(_.iterator().asScala).toList
    (statements ++ prepared ++ List(psInsert, psUpsert).filter(_ != null)).distinct
      .foreach(s => cleanup(s.close()))
    cache.clear()
    statements = Nil
    psInsert = null
    psUpsert = null
    if joinedTo.isEmpty then cleanup(connectionManager.releaseConnection(this))
    released = true
    if (failure != null) throw failure
  }}
}
