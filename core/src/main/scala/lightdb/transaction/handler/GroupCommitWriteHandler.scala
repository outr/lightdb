package lightdb.transaction.handler

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.WriteHandler
import rapid.Task

import scala.collection.mutable

/**
 * Holds a group-commit transaction's writes until its group commits. Nothing reaches the store before then: `flush`
 * and `close` keep the buffer, and the store's commit takes it with [[drain]]. Reads by id see the buffer; queries
 * and scans do not.
 *
 * Writes to one id coalesce: a delete followed by an insert becomes an upsert, so the insert is not refused for the
 * row the same transaction deleted.
 */
class GroupCommitWriteHandler[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends WriteHandler[Doc, Model] {
  private val buffer = mutable.LinkedHashMap.empty[Id[Doc], WriteOp[Doc]]

  override def write(op: WriteOp[Doc]): Task[Unit] = Task {
    synchronized {
      val coalesced = (buffer.get(op.id), op) match {
        case (Some(WriteOp.Delete(_)), WriteOp.Insert(doc)) => WriteOp.Upsert(doc)
        case _ => op
      }
      buffer.remove(op.id)
      buffer.put(op.id, coalesced)
    }
  }

  override def get(id: Id[Doc]): Task[Option[Option[Doc]]] = Task {
    synchronized(buffer.get(id)) match {
      case None => None
      case Some(WriteOp.Delete(_)) => Some(None)
      case Some(WriteOp.Insert(doc)) => Some(Some(doc))
      case Some(WriteOp.Upsert(doc)) => Some(Some(doc))
    }
  }

  /** The buffered writes in the order they were first made, leaving the buffer empty. */
  def drain(): List[WriteOp[Doc]] = synchronized {
    val ops = buffer.values.toList
    buffer.clear()
    ops
  }

  /** The buffered writes, leaving them buffered. */
  def ops: List[WriteOp[Doc]] = synchronized(buffer.values.toList)

  def size: Int = synchronized(buffer.size)

  override def flush: Task[Unit] = Task.unit

  override def clear: Task[Unit] = Task(synchronized(buffer.clear()))

  override def close: Task[Unit] = Task.unit
}
