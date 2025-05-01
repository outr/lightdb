package lightdb.store

import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.transaction.{Transaction, TransactionFeature, TransactionKey}
import rapid.{Task, Unique}

trait BufferedWritingStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Store[Doc, Model] {
  private val id = Unique()

  protected def maxTransactionWriteBuffer: Int = BufferedWritingStore.MaxTransactionWriteBuffer

  protected def getWriteBuffer(transaction: Transaction[Doc, _ <: Model]): WriteBuffer[Doc]
  protected def setWriteBuffer(writeBuffer: WriteBuffer[Doc], transaction: Transaction[Doc, _ <: Model]): Unit

  protected def withWriteBuffer(f: WriteBuffer[Doc] => Task[WriteBuffer[Doc]])
                               (implicit transaction: Transaction[Doc, _ <: Model]): Task[WriteBuffer[Doc]] = Task {
    transaction.synchronized {
      val wb = getWriteBuffer(transaction)
      val modified = f(wb).sync()
      setWriteBuffer(modified, transaction)
      modified
    }
  }

  final protected def flushMap(map: Map[Id[Doc], WriteOp[Doc]]): Task[WriteBuffer[Doc]] = Task.defer {
    val stream = rapid.Stream.fromIterator(Task(map.valuesIterator))
    flushBuffer(stream).map(_ => WriteBuffer[Doc]())
  }

  protected def flushBuffer(stream: rapid.Stream[WriteOp[Doc]]): Task[Unit]

  protected def writeMod(f: Map[Id[Doc], WriteOp[Doc]] => Task[Map[Id[Doc], WriteOp[Doc]]])
                        (implicit transaction: Transaction[Doc, _ <: Model]): Task[WriteBuffer[Doc]] = withWriteBuffer { b =>
    val s = b.map.size
    f(b.map).map { map =>
      val d = map.size - s
      if (b.map.size > maxTransactionWriteBuffer) {
        flushMap(map).sync()
      } else {
        b.copy(map, d)
      }
    }
  }

  override def prepareTransaction(transaction: Transaction[Doc, _ <: Model]): Task[Unit] = Task {
    transaction.put(TransactionKey(id), new TransactionFeature {
      override def commit(): Task[Unit] = {
        writeMod { map =>
          flushMap(map).map(_.map)
        }(transaction).flatMap(_ => super.commit())
      }
    })
  }

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc, _ <: Model]): Task[Doc] = writeMod { map =>
    Task(map + (doc._id -> WriteOp.Insert(doc)))
  }.map(_ => doc)

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc, _ <: Model]): Task[Doc] = writeMod { map =>
    Task(map + (doc._id -> WriteOp.Upsert(doc)))
  }.map(_ => doc)

  protected def _exists(id: Id[Doc])(implicit transaction: Transaction[Doc, _ <: Model]): Task[Boolean]

  override final def exists(id: Id[Doc])(implicit transaction: Transaction[Doc, _ <: Model]): Task[Boolean] = Task.defer {
    val wb = getWriteBuffer(transaction)
    wb.map.get(id) match {
      case Some(op) => op match {
        case _: WriteOp.Insert[Doc] => Task.pure(true)
        case _: WriteOp.Upsert[Doc] => Task.pure(true)
        case _: WriteOp.Delete[Doc] => Task.pure(false)
      }
      case None => _exists(id)
    }
  }

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V)
                                   (implicit transaction: Transaction[Doc, _ <: Model]): Task[Boolean] = if (index == idField) {
    writeMod { map =>
      Task {
        val id = value.asInstanceOf[Id[Doc]]
        map + (id -> WriteOp.Delete(id))
      }
    }.map(_.delta > 0)
  } else {
    throw new UnsupportedOperationException(s"BufferedWritingStore can only get on _id, but ${index.name} was attempted")
  }

  protected def _truncate(implicit transaction: Transaction[Doc, _ <: Model]): Task[Int]

  override final def truncate()(implicit transaction: Transaction[Doc, _ <: Model]): Task[Int] = Task.defer {
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

  override protected def doDispose(): Task[Unit] = super.doDispose()
}

object BufferedWritingStore {
  var MaxTransactionWriteBuffer: Int = 20000
}