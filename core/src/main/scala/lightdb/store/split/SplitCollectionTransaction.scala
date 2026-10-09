package lightdb.store.split

import fabric.Json
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{AtomicBatch, AtomicWriteStore, Collection, Store}
import lightdb.store.write.WriteOp
import lightdb.transaction.{CollectionTransaction, RollbackSupport, Transaction, UndoableTransaction}
import lightdb.transaction.handler.GroupCommitWriteHandler
import lightdb.{Query, SearchResults}
import rapid.{Task, taskSeq2Ops}

/**
 * A transaction on a split collection.
 *
 * Write-through (the default): each write goes to storage, then to the search index, as it is made. A rollback puts
 * storage back as it was (on storage that keeps an undo log, [[UndoableTransaction]]) and restores the touched index
 * rows from it.
 *
 * Group commit (the store opted in, or the transaction was created `withGroupCommit`, or it joined another): writes wait
 * in the transaction, invisible to every other transaction, and reads by id see them. The owner's commit applies its
 * writes and every joined transaction's in one atomic storage write ([[AtomicWriteStore]]), then updates the search
 * index, so the index never holds a row storage lacks. A rollback discards the buffer. An index error after the storage
 * write counts as committed: the rows are durable, the commit succeeds, and the collection re-indexes them in process
 * ([[SplitCollection.indexPending]]); queries miss them until it lands.
 */
case class SplitCollectionTransaction[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model],
](store: SplitCollection[Doc, Model, Storage, Searching],
  parent: Option[Transaction[Doc, Model]],
  writeHandlerFactory: Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]) extends CollectionTransaction[Doc, Model] with RollbackSupport[Doc, Model] {
  override lazy val writeHandler: lightdb.transaction.WriteHandler[Doc, Model] = writeHandlerFactory(this)

  private[split] var _storage: store.storage.TX = _
  private[split] var _searching: store.searching.TX = _

  def storage: store.storage.TX = _storage

  def searching: store.searching.TX = _searching

  /**
   * Defines the mode for how updates apply to the search collection. Defaults to Immediate.
   */
  var searchUpdateHandler: SearchUpdateHandler[Doc, Model, Storage, Searching] = ImmediateUpdateHandler(this)

  def disableSearchUpdate(): Unit = searchUpdateHandler = DisabledUpdateHandler(this)

  override def jsonStream: rapid.Stream[Json] = storage.jsonStream

  @volatile private var undoRecorded = false

  // A write-through write that a rollback must be able to take back.
  private def recordUndo(): Unit = if !undoRecorded then {
    undoRecorded = true
    storage match {
      case u: UndoableTransaction[?, ?] => u.recordUndo()
      case _ => ()
    }
  }

  // The search index hears of each change before storage does, so an index that lags its storage knows it may be
  // behind before the storage write can survive a crash.
  override def _delete(id: Id[Doc]): Task[Boolean] = Task(recordUndo()).next(
    searching.beforeStorageChange(Some(id)).next(searchUpdateHandler.delete(id)).next(storage.delete(id)))

  override protected def _get[V](index: UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = if index == store.idField then {
    storage.get(value.asInstanceOf[Id[Doc]])
  } else {
    Task(store.checkIndex()).next(searching.get(_ => index -> value))
  }

  override protected def _insert(doc: Doc): Task[Doc] = Task(recordUndo()).next(
    searching.beforeStorageChange(Some(doc._id)).next(storage.insert(doc)).flatTap { _ =>
      searchUpdateHandler.insert(doc)
    })

  override protected def _upsert(doc: Doc): Task[Doc] = Task(recordUndo()).next(
    searching.beforeStorageChange(Some(doc._id)).next(storage.upsert(doc)).flatTap { _ =>
      searchUpdateHandler.upsert(doc)
    })

  override protected def _exists(id: Id[Doc]): Task[Boolean] = storage.exists(id)

  override protected def _count: Task[Int] = storage.count

  override def estimatedCount: Task[Int] = storage.estimatedCount

  override protected def _commit: Task[Unit] =
    (if isGroupCommit then commitGroup else Task.unit)
      .next(storage.commit)
      .next(searchUpdateHandler.commit)

  override protected def _rollback: Task[Unit] =
    storage.rollback.guarantee(searchUpdateHandler.rollback).guarantee(searching.rollback)

  override protected def _close: Task[Unit] =
    store.storage.transaction.release(storage)
      .guarantee(searchUpdateHandler.close)
      .guarantee(store.searching.transaction.release(searching))

  // -- Group commit -------------------------------------------------------------------------------------------------

  private def buffer: GroupCommitWriteHandler[Doc, Model] = writeHandler match {
    case g: GroupCommitWriteHandler[Doc, Model] @unchecked => g
    case other => throw new IllegalStateException(s"group-commit transaction on ${store.name} has write handler ${other.getClass.getName}")
  }

  @volatile private var groupOps: List[WriteOp[Doc]] = Nil
  @volatile private var indexTx: Option[store.searching.TX] = None

  private def takeGroupOps(): Unit = groupOps = buffer.drain()

  private def atomicBatch: Option[AtomicBatch[?, ?]] =
    if groupOps.isEmpty then None
    else Some(AtomicBatch(store.storage.asInstanceOf[AtomicWriteStore[Doc, Model]], groupOps))

  // Before the storage write: the index hears of every id, so one that lags its storage knows it may be behind.
  private def prepareIndex: Task[Unit] =
    if groupOps.isEmpty then Task.unit
    else store.searching.transaction.create().flatMap { tx =>
      indexTx = Some(tx)
      groupOps.map(op => tx.beforeStorageChange(Some(op.id))).tasks.unit
    }

  private def abortIndex: Task[Unit] = indexTx match {
    case Some(tx) =>
      indexTx = None
      groupOps = Nil
      store.searching.transaction.abort(tx)
    case None => Task.unit
  }

  // After the storage write: the rows are committed whatever happens here; an index error leaves them to the
  // collection's in-process re-index.
  private def applyIndex: Task[Unit] = indexTx match {
    case None => Task.unit
    case Some(tx) =>
      indexTx = None
      val ops = groupOps
      groupOps = Nil
      Task.defer {
        ops.map {
          case WriteOp.Insert(doc) => tx.upsert(doc).unit
          case WriteOp.Upsert(doc) => tx.upsert(doc).unit
          case WriteOp.Delete(id) => tx.delete(id).unit
        }.tasks.unit
      }.attempt.flatMap {
        case scala.util.Success(_) => store.searching.transaction.release(tx).attempt.map(_.failed.toOption)
        case scala.util.Failure(error) => store.searching.transaction.abort(tx).attempt.map(_ => Some(error))
      }.flatMap {
        case None => Task.unit
        case Some(error) => store.indexFailed(ops.map(_.id), error)
      }.map { _ =>
        ops.foreach {
          case WriteOp.Insert(doc) => trackCacheWrite(doc._id, Some(doc))
          case WriteOp.Upsert(doc) => trackCacheWrite(doc._id, Some(doc))
          case WriteOp.Delete(id) => trackCacheWrite(id, None)
        }
      }
  }

  // The owner applies its writes and every joined transaction's in one atomic storage write, then the index.
  private def commitGroup: Task[Unit] = Task.defer {
    val joined: List[SplitCollectionTransaction[?, ?, ?, ?]] = group.toList.flatMap(_.staged).collect {
      case m: SplitCollectionTransaction[?, ?, ?, ?] => m
    }
    val members: List[SplitCollectionTransaction[?, ?, ?, ?]] = this :: joined
    members.foreach(_.takeGroupOps())
    val batches: List[AtomicBatch[?, ?]] = members.flatMap(m => m.atomicBatch.toList)
    if batches.isEmpty then Task.unit
    else {
      val atomic = store.storage.asInstanceOf[AtomicWriteStore[Doc, Model]]
      members.map(_.prepareIndex).tasks
        .next(atomic.writeAtomically(batches))
        .attempt
        .flatMap {
          case scala.util.Success(_) => members.map(_.applyIndex).tasks.unit
          case scala.util.Failure(error) => members.map(_.abortIndex.attempt).tasks.next(Task.error(error))
        }
    }
  }

  // -- Queries: the search half --------------------------------------------------------------------------------------

  override def doSearch[V](query: Query[Doc, Model, V]): Task[SearchResults[Doc, Model, V]] =
    Task(store.checkIndex()).next(searching.doSearch(query))

  override def streamScored[V](query: Query[Doc, Model, V]): rapid.Stream[(V, Double)] = {
    store.checkIndex()
    searching.streamScored(query.copy(transaction = searching))
  }

  override def aggregate(query: AggregateQuery[Doc, Model]): rapid.Stream[MaterializedAggregate[Doc, Model]] = {
    store.checkIndex()
    searching.aggregate(query)
  }

  override def aggregateCount(query: AggregateQuery[Doc, Model]): Task[Int] =
    Task(store.checkIndex()).next(searching.aggregateCount(query))

  override def distinct[F](query: Query[Doc, Model, _],
                           field: lightdb.field.Field[Doc, F],
                           pageSize: Int): rapid.Stream[F] = {
    store.checkIndex()
    searching.distinct(query.copy(transaction = searching), field, pageSize)
  }

  /** Applied at once in either mode, and not undone by a rollback. A group-commit transaction's buffered writes go
    * with it. */
  override def truncate: Task[Int] = (if isGroupCommit then buffer.clear else Task.unit)
    .next(searching.beforeStorageChange(None))
    .next(storage.truncate).flatTap { _ =>
      searchUpdateHandler.truncate
    }
}
