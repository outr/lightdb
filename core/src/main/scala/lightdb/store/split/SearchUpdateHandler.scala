package lightdb.store.split

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.{Collection, Store}
import rapid.{Task, logger}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.CollectionHasAsScala

trait SearchUpdateHandler[
  Doc <: Document[Doc],
  Model <: DocumentModel[Doc],
  Storage <: Store[Doc, Model],
  Searching <: Collection[Doc, Model],
] {
  def txn: SplitCollectionTransaction[Doc, Model, Storage, Searching]

  def insert(doc: Doc): Task[Unit]
  def upsert(doc: Doc): Task[Unit]
  def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Unit]
  def commit: Task[Unit]
  def rollback: Task[Unit]
  def truncate: Task[Unit]
  def close: Task[Unit]
}

/**
 * SearchUpdateMode is the directive used in SplitCollectionTransaction to handle how updates are applied to the search
 * collection. Defaults to Immediate.
 */
object SearchUpdateHandler {
  /**
   * Applies search updates immediately as a delta occurs. Leads to blocking and slower updates.
   *
   * This is the default operation in a SplitCollectionTransaction.
   */
  case class Immediate[
    Doc <: Document[Doc],
    Model <: DocumentModel[Doc],
    Storage <: Store[Doc, Model],
    Searching <: Collection[Doc, Model],
  ](txn: SplitCollectionTransaction[Doc, Model, Storage, Searching]) extends SearchUpdateHandler[Doc, Model, Storage, Searching] {
    override def insert(doc: Doc): Task[Unit] = txn.searching.insert(doc).unit
    override def upsert(doc: Doc): Task[Unit] = txn.searching.upsert(doc).unit
    override def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Unit] = txn.searching.delete(_ => index -> value).unit
    override def commit: Task[Unit] = txn.searching.commit
    override def rollback: Task[Unit] = txn.searching.rollback
    override def truncate: Task[Unit] = txn.searching.truncate.unit
    override def close: Task[Unit] = Task.unit
  }

  /**
   * Concurrently applies updates in the transaction. Will block until complete during transaction commit.
   *
   * Note: This will keep all objects in memory while they are being processed. This can lead to out-of-memory errors if
   * updates are going significantly faster than they can be applied.
   *
   * This can lead to great performance improvements, but unexpectedly long commit delays.
   */
  case class AsynchronousCached[
    Doc <: Document[Doc],
    Model <: DocumentModel[Doc],
    Storage <: Store[Doc, Model],
    Searching <: Collection[Doc, Model],
  ](txn: SplitCollectionTransaction[Doc, Model, Storage, Searching],
    maxCache: Int = 5_000,
    monitorDelay: FiniteDuration = 250.millis) extends SearchUpdateHandler[Doc, Model, Storage, Searching] {
    @volatile private var keepAlive = true
    private val cached = new AtomicInteger(0)
    private val queue = new ConcurrentLinkedQueue[Task[Unit]]

    Task {
      while (keepAlive || !queue.isEmpty) {
        Option(queue.poll()) match {
          case Some(task) =>
            try {
              task.function(cached.decrementAndGet()).sync()
            } catch {
              case t: Throwable => logger.error("Error processing search update", t)
            }
          case None => Task.sleep(monitorDelay).sync()
        }
      }
    }.start()

    private def add(task: Task[Unit]): Task[Unit] = Task.defer {
      if (cached.get() >= maxCache) {
        Task.sleep(250.millis).next(add(task))
      } else {
        cached.incrementAndGet()
        queue.add(task)
        Task.unit
      }
    }

    override def insert(doc: Doc): Task[Unit] = add(txn.searching.insert(doc).unit)
    override def upsert(doc: Doc): Task[Unit] = add(txn.searching.upsert(doc).unit)
    override def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Unit] =
      add(txn.searching.delete(_ => index -> value).unit)
    override def commit: Task[Unit] = add(txn.searching.commit)
      .condition(Task(cached.get() <= 0))
    override def rollback: Task[Unit] = add(txn.searching.rollback)
    override def truncate: Task[Unit] = add(txn.searching.truncate.unit)
    override def close: Task[Unit] = Task.function {
      keepAlive = false
    }.condition(Task(cached.get() <= 0)).next(txn.searching.commit)
  }

  /**
   * Doesn't apply any search updates until transaction commit. This is the fastest and most efficient application when
   * making many changes to the same documents, but leads to maximum delays during commit.
   */
  case class Queued[
    Doc <: Document[Doc],
    Model <: DocumentModel[Doc],
    Storage <: Store[Doc, Model],
    Searching <: Collection[Doc, Model],
  ](txn: SplitCollectionTransaction[Doc, Model, Storage, Searching]) extends SearchUpdateHandler[Doc, Model, Storage, Searching] {
    private val deltas = new ConcurrentHashMap[Id[Doc], QueuedDelta]

    override def insert(doc: Doc): Task[Unit] = Task.defer {
      val delta = Option(deltas.get(doc._id)) match {
        case Some(QueuedDelta.Delete(_)) => QueuedDelta.Upsert(doc._id)
        case _ => QueuedDelta.Insert(doc._id)
      }
      deltas.put(doc._id, delta)
      Task.unit
    }

    override def upsert(doc: Doc): Task[Unit] = Task.defer {
      val delta = Option(deltas.get(doc._id)) match {
        case Some(QueuedDelta.Insert(_)) => QueuedDelta.Insert(doc._id)
        case Some(QueuedDelta.Delete(_)) => QueuedDelta.Insert(doc._id)
        case _ => QueuedDelta.Upsert(doc._id)
      }
      deltas.put(doc._id, delta)
      Task.unit
    }

    override def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Unit] = Task.defer {
      if (index != txn.store.model._id) throw new UnsupportedOperationException("Only id deletes are supported in SearchUpdateHandler.Queued")
      val id = value.asInstanceOf[Id[Doc]]
      val delta = Option(deltas.get(id)) match {
        case Some(QueuedDelta.Insert(_)) => None
        case _ => Some(QueuedDelta.Delete(id))
      }
      delta.foreach { d =>
        deltas.put(id, d)
      }
      Task.unit
    }

    override def commit: Task[Unit] = rapid.Stream.fromIterator(Task(deltas.values().asScala.iterator))
      .evalMap {
        case QueuedDelta.Insert(id) => txn.storage.get(id).flatMap {
          case Some(doc) => txn.searching.insert(doc)
          case None => logger.warn(s"Failed to find $id")
        }
        case QueuedDelta.Upsert(id) => txn.storage.get(id).flatMap {
          case Some(doc) => txn.searching.upsert(doc)
          case None => logger.warn(s"Failed to find $id")
        }
        case QueuedDelta.Delete(id) => txn.searching.delete(id)
      }
      .drain
      .map(_ => deltas.clear())
    override def rollback: Task[Unit] = Task.defer {
      deltas.clear()
      txn.searching.rollback
    }
    override def truncate: Task[Unit] = txn.searching.truncate.unit

    override def close: Task[Unit] = Task.unit

    sealed trait QueuedDelta

    object QueuedDelta {
      case class Insert(id: Id[Doc]) extends QueuedDelta
      case class Upsert(id: Id[Doc]) extends QueuedDelta
      case class Delete(id: Id[Doc]) extends QueuedDelta
    }
  }

  /**
   * Ignores deltas causing search to get further and further out-of-sync with with storage.
   *
   * This can be useful when many updates need to occur across multiple transactions and then reIndex is called when
   * finished.
   */
  case class Disabled[
    Doc <: Document[Doc],
    Model <: DocumentModel[Doc],
    Storage <: Store[Doc, Model],
    Searching <: Collection[Doc, Model],
  ](txn: SplitCollectionTransaction[Doc, Model, Storage, Searching]) extends SearchUpdateHandler[Doc, Model, Storage, Searching] {
    override def insert(doc: Doc): Task[Unit] = Task.unit
    override def upsert(doc: Doc): Task[Unit] = Task.unit
    override def delete[V](index: UniqueIndex[Doc, V], value: V): Task[Unit] = Task.unit
    override def commit: Task[Unit] = Task.unit
    override def rollback: Task[Unit] = Task.unit
    override def truncate: Task[Unit] = Task.unit
    override def close: Task[Unit] = Task.unit
  }
}