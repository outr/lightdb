package lightdb.store.split

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field.UniqueIndex
import lightdb.id.Id
import lightdb.store.{Collection, Store}
import rapid.{Task, logger}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Doesn't apply any search updates until transaction commit. This is the fastest and most efficient application when
 * making many changes to the same documents, but leads to maximum delays during commit.
 */
case class QueuedUpdateHandler[
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
    if (index != txn.store.model._id) throw new UnsupportedOperationException("Only id deletes are supported in QueuedUpdateHandler")
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

  private object QueuedDelta {
    case class Insert(id: Id[Doc]) extends QueuedDelta
    case class Upsert(id: Id[Doc]) extends QueuedDelta
    case class Delete(id: Id[Doc]) extends QueuedDelta
  }
}