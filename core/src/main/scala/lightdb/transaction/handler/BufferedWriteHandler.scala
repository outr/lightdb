package lightdb.transaction.handler

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.WriteHandler
import rapid.Task

import scala.collection.compat.immutable.ArraySeq
import scala.collection.mutable

class BufferedWriteHandler[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    maxBufferSize: Int,
    flushOps: Seq[WriteOp[Doc]] => Task[Unit]
) extends WriteHandler[Doc, Model] {
  private val lock = new AnyRef
  private val buffer = new mutable.HashMap[Id[Doc], WriteOp[Doc]]()

  override def write(op: WriteOp[Doc]): Task[Unit] = Task.defer {
    val overflow = lock.synchronized {
      buffer.update(op.id, op)
      buffer.size > maxBufferSize
    }
    if (overflow) flush else Task.unit
  }

  override def get(id: Id[Doc]): Task[Option[Option[Doc]]] = Task {
    lock.synchronized {
      buffer.get(id).map {
        case WriteOp.Delete(_) => None
        case WriteOp.Insert(doc) => Some(doc)
        case WriteOp.Upsert(doc) => Some(doc)
      }
    }
  }

  override def flush: Task[Unit] = Task.defer {
    val ops = lock.synchronized {
      if (buffer.isEmpty) None
      else {
        val values = ArraySeq.from(buffer.valuesIterator)
        buffer.clear()
        Some(values)
      }
    }
    ops match {
      case None => Task.unit
      case Some(values) => flushOps(values)
    }
  }

  override def clear: Task[Unit] = Task {
    lock.synchronized {
      buffer.clear()
    }
  }

  override def close: Task[Unit] = flush
}
