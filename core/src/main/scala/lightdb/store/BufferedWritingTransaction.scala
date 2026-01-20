package lightdb.store

import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.transaction.Transaction
import rapid._

trait BufferedWritingTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Transaction[Doc, Model] { transaction =>
  protected def maxTransactionWriteBuffer: Int = BufferedWritingTransaction.MaxTransactionWriteBuffer

  private var writeBuffer: WriteBuffer[Doc] = WriteBuffer()

  protected def withWriteBuffer(f: WriteBuffer[Doc] => Task[WriteBuffer[Doc]]): Task[WriteBuffer[Doc]] = Task {
    transaction.synchronized {
      val wb = writeBuffer
      val modified = f(wb).sync()
      writeBuffer = modified
      modified
    }
  }

  final protected def flushMap(map: Map[Id[Doc], WriteOp[Doc]]): Task[WriteBuffer[Doc]] = Task.defer {
    val stream = rapid.Stream.fromIterator(Task(map.valuesIterator))
    flushBuffer(stream).map(_ => WriteBuffer[Doc]())
  }

  protected def flushBuffer(stream: rapid.Stream[WriteOp[Doc]]): Task[Unit]

  protected def writeMod(f: Map[Id[Doc], WriteOp[Doc]] => Task[Map[Id[Doc], WriteOp[Doc]]]): Task[WriteBuffer[Doc]] = withWriteBuffer { b =>
    val s = b.map.size
    f(b.map).flatMap { map =>
      val d = map.size - s
      if (map.size > maxTransactionWriteBuffer) {
        flushMap(map)
      } else {
        Task(b.copy(map, d))
      }
    }
  }

  protected def directGet[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]]

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task.defer {
    if (index == store.idField) {
      writeBuffer.map.get(value.asInstanceOf[Id[Doc]]) match {
        case Some(WriteOp.Delete(_)) => Task.pure(None)
        case Some(WriteOp.Insert(doc)) => Task.pure(Some(doc))
        case Some(WriteOp.Upsert(doc)) => Task.pure(Some(doc))
        case None => directGet(index, value)
      }
    } else {
      throw new UnsupportedOperationException(s"HaloDBStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override protected def _insert(doc: Doc): Task[Doc] = writeMod { map =>
    Task(map + (doc._id -> WriteOp.Insert(doc)))
  }.map(_ => doc)

  override protected def _upsert(doc: Doc): Task[Doc] = writeMod { map =>
    Task(map + (doc._id -> WriteOp.Upsert(doc)))
  }.map(_ => doc)

  override protected def _exists(id: Id[Doc]): Task[Boolean] = _get(store.idField, id).map(_.nonEmpty)

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] = if (index == store.idField) {
    writeMod { map =>
      Task {
        val id = value.asInstanceOf[Id[Doc]]
        map + (id -> WriteOp.Delete(id))
      }
    }.map(_.delta > 0)
  } else {
    throw new UnsupportedOperationException(s"BufferedWritingStore can only get on _id, but ${index.name} was attempted")
  }

  override protected def _commit: Task[Unit] = writeMod { map =>
    flushMap(map).map(_.map)
  }.unit

  override protected def _rollback: Task[Unit] = writeMod { _ =>
    Task(Map.empty)
  }.unit

  override protected def _close: Task[Unit] = Task.unit

  protected def _truncate: Task[Int]

  override def truncate: Task[Int] = Task.defer {
    var count = 0
    withWriteBuffer { wb =>
      Task {
        count = wb.map.size
        wb.copy(Map.empty)
      }
    }.flatMap { _ =>
      _truncate.map(c => c + count)
    }
  }
}

object BufferedWritingTransaction {
  var MaxTransactionWriteBuffer: Int = 20000
}