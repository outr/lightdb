package lightdb.transaction

import fabric.*
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import lightdb.doc.{Document, DocumentModel}
import lightdb.error.{DocNotFoundException, DuplicateIdException}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.Store
import lightdb.store.write.WriteOp
import rapid.*

import java.util.concurrent.ConcurrentHashMap

trait Transaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] {
  def store: Store[Doc, Model]
  def parent: Option[Transaction[Doc, Model]]
  def writeHandler: WriteHandler[Doc, Model]

  @volatile private var rolledBack = false
  final def isRolledBack: Boolean = rolledBack

  @volatile private var closed = false
  /** Whether [[close]] has run: the transaction is released and accepts no further writes. */
  final def isClosed: Boolean = closed

  /** A write after release would go to a closed write handler and be lost without an error. A write in a
    * group-commit transaction counts against the group's bound and fails loud past it. */
  private def writable[A](task: => Task[A]): Task[A] = Task.defer {
    if (closed) Task.error(new IllegalStateException(s"transaction for ${store.name} used after release"))
    else {
      group.foreach(_.countWrite(this))
      task
    }
  }

  /** Flag the transaction as aborting before end hooks are notified, so a hook that manages a
    * dependent transaction (e.g. a reverse-edge store) can abort it rather than commit it. */
  private[lightdb] final def markRolledBack(): Unit = rolledBack = true

  @volatile private var _abortCause: Option[Throwable] = None

  /** Why this transaction was rolled back by its group (a failed joined transaction, a write past the group's bound),
    * so its release fails with it rather than returning as if it had committed. */
  final def abortCause: Option[Throwable] = _abortCause

  /** Rolls the transaction back because of `cause`; its release then fails with it. */
  private[lightdb] final def rollBackFor(cause: Throwable): Unit = synchronized {
    rolledBack = true
    if _abortCause.isEmpty then _abortCause = Some(cause)
  }

  // -- Joined transactions and group commit ----------------------------------------------------------------------

  @volatile private var _joinOwner: Option[Transaction[?, ?]] = None
  @volatile private var _group: Option[TransactionGroup] = None
  private val joinOpen = new java.util.concurrent.atomic.AtomicBoolean(false)

  /** The transaction this one joined (`store.transaction.joining(owner)`): its commit or rollback decides this
    * transaction's writes. */
  final def joinOwner: Option[Transaction[?, ?]] = _joinOwner

  final def isJoined: Boolean = _joinOwner.nonEmpty

  /** The group this transaction commits in: its own as an owner (created with a group-commit bound or joined by
    * another), its owner's when joined. */
  final def group: Option[TransactionGroup] = _joinOwner match {
    case Some(owner) => owner.group
    case None => _group
  }

  /** Whether this transaction holds its writes for one group commit: it was created with a group-commit bound, or it
    * joined another transaction. */
  final def isGroupCommit: Boolean = isJoined || _group.exists(_.bound.nonEmpty)

  /** The group this transaction owns, created on first need. */
  private[lightdb] final def ownGroup: TransactionGroup = synchronized {
    _group.getOrElse {
      val g = new TransactionGroup(this, None)
      _group = Some(g)
      g
    }
  }

  private[lightdb] final def initGroup(bound: Int): Unit = synchronized {
    _group = Some(new TransactionGroup(this, Some(bound)))
  }

  private[lightdb] final def attachTo(owner: Transaction[?, ?]): Unit = {
    _joinOwner = Some(owner)
    owner.ownGroup.joinOpened()
    joinOpen.set(true)
  }

  /** The joined transaction ended without being staged (an abort): it no longer holds its owner's commit open. */
  private[lightdb] final def detachOpenJoin(): Unit =
    if joinOpen.compareAndSet(true, false) then _joinOwner.foreach(_.group.foreach(_.joinClosed()))

  /** Releases a joined transaction: its writes are handed to the owner's group (see [[_stage]]) and committed or
    * discarded with the owner. */
  private[lightdb] final def stage: Task[Unit] = Task.defer {
    val owner = _joinOwner.getOrElse(throw new IllegalStateException(s"transaction for ${store.name} is not joined"))
    if (rolledBack) Task.error(new IllegalStateException("Cannot commit a rolled-back transaction"))
    else if (owner.isRolledBack || owner.isClosed)
      Task.error(new IllegalStateException(s"transaction for ${store.name} joined a transaction on ${owner.store.name} that has already ended"))
    else writeHandler.flush.next(_stage)
  }

  private[lightdb] final def staged(outcome: Boolean => Task[Unit]): Unit =
    if joinOpen.compareAndSet(true, false) then _joinOwner.foreach(_.ownGroup.stage(this, outcome))

  /** A joined transaction's writes leave it: the backend makes them part of the owner's commit. Called at release,
    * before the owner commits. A buffering backend leaves them where its owner's commit collects them. */
  protected def _stage: Task[Unit] = Task.unit

  /** A failure on a joined transaction aborts its owner too: the group commits whole or not at all. */
  private[lightdb] final def failGroup(cause: Throwable): Unit = {
    rollBackFor(cause)
    _joinOwner.foreach(_.rollBackFor(new IllegalStateException(s"a transaction on ${store.name} joined to this one failed", cause)))
  }

  /** The owner of a joined transaction committed: apply its pending cache writes. */
  private[lightdb] final def joinedCommitted(): Unit = applyCachePendingNow()

  /** The owner of a joined transaction rolled back: discard its pending writes. */
  private[lightdb] final def joinedRolledBack(): Task[Unit] = Task.defer {
    rolledBack = true
    writeHandler.clear.guarantee(Task(cachePending.clear()))
  }

  /** Terminal abort. Stop pending/asynchronous writes before rolling back the backend. Backends
    * without transactional storage cannot undo already-applied writes; this is not distributed ACID. */
  final def rollback: Task[Unit] = Task.defer {
    rolledBack = true
    writeHandler.abort.guarantee(_rollback).guarantee(Task(cachePending.clear()))
  }

  // -- Tx-local cache overlay -----------------------------------------------------------------------------
  // Writes in this transaction are accumulated here and applied to `store.cache` on commit. On rollback
  // the overlay is discarded, so rolled-back writes never reach the store cache.
  //
  // Values:
  //   Some(doc) — the doc was inserted or upserted
  //   None      — the id was deleted (tombstone)
  //
  // Populated via `applyWriteOps` (below). No-op when `store.cache.isEmpty`.
  private[lightdb] lazy val cachePending: ConcurrentHashMap[Id[Doc], Option[Doc]] =
    new ConcurrentHashMap[Id[Doc], Option[Doc]]()

  /**
   * Strict insert: fails with [[lightdb.error.DuplicateIdException]] if `doc._id` already
   * exists. Use [[upsert]] for create-or-replace semantics.
   *
   * With a buffered/queued/async write handler the duplicate is detected at flush time rather
   * than at the call site — wrap in `.next(transaction.flush)` if the caller needs the error
   * to surface synchronously.
   */
  final def insert(doc: Doc): Task[Doc] = writable(store.trigger.insert(doc, this)
    .next(writeHandler.write(WriteOp.Insert(doc)))
    .map(_ => doc))

  /**
   * Optimized insert for handling large streams of documents.
   *
   * @param stream the stream to insert
   * @param commitEvery if set, will create a transaction per batch to avoid overflowing transactions with too many
   *                    inserts.
   * @return the insertion count
   */
  def insert(stream: rapid.Stream[Doc], commitEvery: Option[Int] = None): Task[Int] = commitEvery match {
    case Some(chunkSize) => stream
      .chunk(chunkSize)
      .evalMap { chunk =>
        store.transaction { writeTransaction =>
          writeTransaction.insert(chunk).map(_.size)
        }
      }
      .sum
    case None => stream.evalMap(insert).count.flatTap(_ => flush)
  }

  final def insert(docs: Seq[Doc]): Task[Seq[Doc]] = insert(rapid.Stream.emits(docs)).map(_ => docs)
  def insertJson(stream: rapid.Stream[Json]): Task[Int] = insert(stream.map(_.as[Doc](store.model.rw)))

  final def upsert(doc: Doc): Task[Doc] = writable(store.trigger.upsert(doc, this)
    .next(writeHandler.write(WriteOp.Upsert(doc)))
    .map(_ => doc))

  /**
   * Write back ONLY [[fields]] for these documents, leaving every other column untouched.
   *
   * The hook a field-scoped re-index writes through ([[lightdb.store.Store.reIndex]]). Backends that
   * can express a partial write override this; the default upserts the whole document, which is
   * correct and simply forgoes the saving, so no backend has to implement it to stay right.
   *
   * The VALUES still come from the documents, computed by the field definitions themselves, so a
   * backend override changes only how the write is issued - never what the value is. That matters:
   * the alternative, teaching each backend to recompute a field in its own dialect, is a second
   * definition of the same thing, and the two drift.
   */
  def updateFields(docs: List[Doc], fields: List[lightdb.field.Field[Doc, ?]]): Task[Int] =
    docs.map(upsert).tasks.map(_.size)

  /**
   * Optimized upsert for handling large streams of documents.
   *
   * @param stream the stream to upsert
   * @param commitEvery if set, will create a transaction per batch to avoid overflowing transactions with too many
   *                    upserts.
   * @return the upsert count
   */
  def upsert(stream: rapid.Stream[Doc], commitEvery: Option[Int] = None): Task[Int] = commitEvery match {
    case Some(chunkSize) => stream
      .chunk(chunkSize)
      .evalMap { chunk =>
        store.transaction { writeTransaction =>
          writeTransaction.upsert(chunk).map(_.size)
        }
      }
      .sum
    case None => stream.evalMap(upsert).count.flatTap(_ => flush)
  }

  final def upsert(docs: Seq[Doc]): Task[Seq[Doc]] = upsert(rapid.Stream.emits(docs)).map(_ => docs)
  def upsertJson(stream: rapid.Stream[Json]): Task[Int] = upsert(stream.map(_.as[Doc](store.model.rw)))

  final def exists(id: Id[Doc]): Task[Boolean] = get(id).map(_.nonEmpty)
  final def count: Task[Int] = _count

  /**
   * Gets an estimated count if supported by database. Falls back to using count.
   */
  def estimatedCount: Task[Int] = count
  def flush: Task[Unit] = writeHandler.flush
  final def commit: Task[Unit] = Task.defer {
    if (rolledBack) Task.error(abortCause.getOrElse(new IllegalStateException("Cannot commit a rolled-back transaction")))
    else if (isJoined) stage
    else group.filter(_.openMembers > 0) match {
      case Some(g) => Task.error(new IllegalStateException(
        s"transaction for ${store.name} cannot commit while ${g.openMembers} transaction(s) joined to it are still open"))
      case None => writeHandler.flush.next(_commit).next(applyCachePending)
    }
  }
  final def close: Task[Unit] = writeHandler.close.guarantee(_close).guarantee(Task { closed = true })

  /**
   * Drain `cachePending` into the store-level cache. Called after `_commit` succeeds so rolled-back or
   * failed transactions never touch the cache. No-op when caching is disabled.
   */
  private def applyCachePending: Task[Unit] = Task(applyCachePendingNow())

  private def applyCachePendingNow(): Unit = {
    store.cache.foreach { cache =>
      if (!cachePending.isEmpty) {
        val it = cachePending.entrySet().iterator()
        while (it.hasNext) {
          val e = it.next()
          e.getValue match {
            case Some(doc) => cache.put(e.getKey, doc)
            case None => cache.evict(e.getKey)
          }
        }
        cachePending.clear()
      }
    }
  }

  def get(id: Id[Doc]): Task[Option[Doc]] = writeHandler.get(id).flatMap {
    case Some(result) => Task.pure(result)
    case None =>
      // Tx-local overlay shadows the store cache for in-flight writes. This protects
      // DirectWriteHandler backends from returning stale cached values after a same-tx upsert/delete
      // (the backend already has the new value, but the store cache hasn't been refreshed yet — that
      // happens on commit). For BufferedWriteHandler backends `writeHandler.get` already covered this
      // above, so the overlay is empty for the same id and we fall straight through.
      val pending = cachePending.get(id)
      if (pending != null) Task.pure(pending)  // Some(doc) = pending insert/upsert; None = pending delete
      else store.cache match {
        case None => _get(store.idField, id)
        case Some(cache) => cache.get(id) match {
          case Some(doc) => Task.pure(Some(doc))
          case None => _get(store.idField, id).map { resultOpt =>
            // Read-through: populate the cache on a backend hit. Misses are not cached.
            resultOpt.foreach(doc => cache.put(id, doc))
            resultOpt
          }
        }
      }
  }
  def getAll(ids: Seq[Id[Doc]]): rapid.Stream[Doc] = rapid.Stream
    .emits(ids)
    .evalMap(apply)
  def apply(id: Id[Doc]): Task[Doc] = get(id).map(_.getOrElse {
    throw DocNotFoundException(store.name, "_id", id)
  })
  def getOrCreate(id: Id[Doc], create: => Doc, establishLock: Boolean = true): Task[Doc] =
    modify(id, establishLock = establishLock) {
      case Some(doc) => Task.pure(Some(doc))
      case None => Task.pure(Some(create))
    }.map(_.get)
  def modify(id: Id[Doc],
             establishLock: Boolean = true,
             deleteOnNone: Boolean = false)
            (f: Forge[Option[Doc], Option[Doc]]): Task[Option[Doc]] = store.lock(id, get(id), establishLock) { existing =>
    f(existing).flatMap {
      case Some(doc) => upsert(doc).map(_ => Some(doc))
      case None if deleteOnNone => delete(id).map(_ => None)
      case None => Task.pure(None)
    }
  }
  final def delete(id: Id[Doc]): Task[Boolean] = writable(store.trigger.delete(id, this)
    .next(writeHandler.write(WriteOp.Delete(id)))
    .map(_ => true))

  def list: Task[List[Doc]] = stream.toList
  def stream: rapid.Stream[Doc] = jsonStream.map(_.as[Doc](store.model.rw))
  def jsonStream: rapid.Stream[Json]
  def truncate: Task[Int]

  /**
   * String-id variants of [[get]] / [[delete]] that emit/accept JSON rather than
   * concrete `Doc` values. Useful for callers that don't know the document type
   * statically — e.g. diagnostic API surfaces operating over `Store[_, _]`.
   */
  def jsonGet(idValue: String): Task[Option[Json]] =
    get(Id[Doc](idValue)).map(_.map(_.json(store.model.rw)))

  def jsonDelete(idValue: String): Task[Boolean] = delete(Id[Doc](idValue))

  private[lightdb] def applyWriteOps(ops: Seq[WriteOp[Doc]]): Task[Unit] =
    ops.map {
      case WriteOp.Insert(doc) => _insert(doc).map(_ => trackCacheWrite(doc._id, Some(doc)))
      case WriteOp.Upsert(doc) => _upsert(doc).map(_ => trackCacheWrite(doc._id, Some(doc)))
      case WriteOp.Delete(id) => _delete(id).map(_ => trackCacheWrite(id, None))
    }.tasks.unit

  /**
   * Record a pending cache mutation for this transaction. The operation is buffered in [[cachePending]]
   * and applied to the store-level cache on commit (or discarded on rollback). No-op when caching is
   * disabled.
   *
   * `protected` so store-native batch write paths (e.g. [[lightdb.sql.SQLStoreTransaction.flushOps]])
   * can keep the point cache consistent too — otherwise a cached SQL store returns stale reads after a
   * write, since those paths bypass [[applyWriteOps]].
   */
  protected def trackCacheWrite(id: Id[Doc], doc: Option[Doc]): Unit =
    if (store.cache.isDefined) cachePending.put(id, doc)

  protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]]

  /**
   * Insert a new document, failing if `doc._id` already exists.
   *
   * Default implementation calls `_exists` and then `_upsert`, which adds an extra read per
   * insert on backends without a native "insert if absent" primitive. Backends that have one
   * (LMDB via `MDB_NOOVERWRITE`, SQL via PK constraints, ChronicleMap via `putIfAbsent`)
   * override this with the cheaper native path. When duplicate-detection is unwanted —
   * for example bulk loads where ids are known unique — call `upsert` instead.
   */
  protected def _insert(doc: Doc): Task[Doc] = _exists(doc._id).flatMap {
    case true => Task.error(DuplicateIdException(store.name, doc._id))
    case false => _upsert(doc)
  }
  protected def _upsert(doc: Doc): Task[Doc]
  protected def _delete(id: Id[Doc]): Task[Boolean]
  protected def _exists(id: Id[Doc]): Task[Boolean]
  protected def _count: Task[Int]
  protected def _commit: Task[Unit]
  protected def _rollback: Task[Unit]
  protected def _close: Task[Unit]

  protected def toString(doc: Doc): String = JsonFormatter.Compact(doc.json(store.model.rw))
  protected def fromString(string: String): Doc = toJson(string).as[Doc](store.model.rw)
  protected def toJson(string: String): Json = JsonParser(string)
}
