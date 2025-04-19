package lightdb.store

import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.transaction.Transaction
import lightdb.util.ElasticHashMap
import rapid.Task

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.CollectionHasAsScala

trait BufferedWritingStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]] extends Store[Doc, Model] {
  protected def maxTransactionWriteBuffer: Int = BufferedWritingStore.MaxTransactionWriteBuffer

  private val transactionWriteBuffers = new ConcurrentHashMap[Transaction[Doc], WriteBuffer]

  final protected def flushMap(map: Map[Id[Doc], WriteOp]): Task[WriteBuffer] = Task.defer {
    val stream = rapid.Stream.fromIterator(Task(map.valuesIterator))
    flushBuffer(stream).map(_ => WriteBuffer())
  }

  protected def flushBuffer(stream: rapid.Stream[WriteOp]): Task[Unit]

  protected def writeMod(f: Map[Id[Doc], WriteOp] => Task[Map[Id[Doc], WriteOp]])
                        (implicit transaction: Transaction[Doc]): Task[WriteBuffer] = Task {
    transactionWriteBuffers.compute(transaction, (_, current) => {
      val b = Option(current).getOrElse(WriteBuffer())
      val s = b.map.size
      val map = f(b.map).sync()
      val d = b.map.size - s
      if (b.map.size > maxTransactionWriteBuffer) {
        flushMap(map).sync()
      } else {
        b.copy(map, d)
      }
    })
  }

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = writeMod { map =>
    Task(map + (doc._id -> WriteOp.Upsert(doc)))
  }.map(_ => doc)

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = _insert(doc)

  abstract override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task.defer {
    Option(transactionWriteBuffers.get(transaction)) match {
      case Some(wb) => wb.map.get(id) match {
        case Some(op) => op match {
          case _: WriteOp.Upsert => Task.pure(true)
          case WriteOp.Delete => Task.pure(false)
        }
        case None => super.exists(id)
      }
      case None => super.exists(id)
    }
  }

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V)
                                   (implicit transaction: Transaction[Doc]): Task[Boolean] = if (index == idField) {
    writeMod { map =>
      Task {
        val id = value.asInstanceOf[Id[Doc]]
        map - id
      }
    }.map(_.delta < 0)
  } else {
    throw new UnsupportedOperationException(s"BufferedWritingStore can only get on _id, but ${index.name} was attempted")
  }

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    val count = transactionWriteBuffers.values().asScala.map(_.map.size).sum
    transactionWriteBuffers.clear()
    count
  }

  override def releaseTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.defer {
    writeMod { map =>
      flushMap(map).map(_.map)
    }(transaction)
  }.flatMap(_ => super.releaseTransaction(transaction))

  override protected def doDispose(): Task[Unit] = super.doDispose()

  case class WriteBuffer(map: Map[Id[Doc], WriteOp] = ElasticHashMap.empty(), delta: Int = 0)

  sealed trait WriteOp

  object WriteOp {
    case class Upsert(doc: Doc) extends WriteOp
    case object Delete extends WriteOp
  }
}

object BufferedWritingStore {
  var MaxTransactionWriteBuffer: Int = 20_000
}