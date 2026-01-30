package lightdb.transaction.handler

import lightdb.doc.{Document, DocumentModel}
import lightdb.id.Id
import lightdb.store.write.WriteOp
import lightdb.transaction.WriteHandler
import rapid.Task

import lightdb.util.StoreMetrics

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.compat.immutable.ArraySeq
import scala.jdk.CollectionConverters.*

class BufferedWriteHandler[Doc <: Document[Doc], Model <: DocumentModel[Doc]](
    maxBufferSize: Int,
    flushOps: Seq[WriteOp[Doc]] => Task[Unit]
) extends WriteHandler[Doc, Model] {
  private val bufferRef = new AtomicReference(new ConcurrentHashMap[Id[Doc], WriteOp[Doc]]())
  private val size = new AtomicInteger(0)

  override def write(op: WriteOp[Doc]): Task[Unit] = Task.defer {
    val buffer = bufferRef.get()
    val previous = buffer.put(op.id, op)
    val overflow = if (previous == null) size.incrementAndGet() > maxBufferSize else size.get() > maxBufferSize
    if (overflow) {
      StoreMetrics.recordBufferedOverflow()
      flush
    } else Task.unit
  }

  override def get(id: Id[Doc]): Task[Option[Option[Doc]]] = Task {
    bufferRef.get().get(id) match {
      case null => None
      case WriteOp.Delete(_) => Some(None)
      case WriteOp.Insert(doc) => Some(Some(doc))
      case WriteOp.Upsert(doc) => Some(Some(doc))
    }
  }

  override def flush: Task[Unit] = Task.defer {
    val buffer = bufferRef.getAndSet(new ConcurrentHashMap[Id[Doc], WriteOp[Doc]]())
    val count = size.getAndSet(0)
    if (count == 0) Task.unit
    else {
      val values = ArraySeq.from(buffer.values().asScala)
      StoreMetrics.recordBufferedFlush(values.size)
      flushOps(values)
    }
  }

  override def clear: Task[Unit] = Task {
    bufferRef.set(new ConcurrentHashMap[Id[Doc], WriteOp[Doc]]())
    size.set(0)
  }

  override def close: Task[Unit] = flush
}
