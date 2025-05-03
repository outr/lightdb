package lightdb.lmdb

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import fabric.{Json, Null}
import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.{BufferedWritingTransaction, WriteBuffer, WriteOp}
import lightdb.transaction.Transaction
import org.lmdbjava.{Env, GetOp, PutFlags, Txn}
import rapid._

import java.nio.ByteBuffer

case class LMDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: LMDBStore[Doc, Model],
                                                                              env: Env[ByteBuffer],
                                                                              parent: Option[Transaction[Doc, Model]]) extends BufferedWritingTransaction[Doc, Model] {
  private var _readTxn: Txn[ByteBuffer] = env.txnRead()

  def readTxn: Txn[ByteBuffer] = _readTxn

  var writeBuffer: WriteBuffer[Doc] = WriteBuffer()

  override protected def flushBuffer(stream: rapid.Stream[WriteOp[Doc]]): Task[Unit] = Task {
    val txn = store.instance.env.txnWrite()
    stream
      .map {
        case WriteOp.Insert(doc) => store.instance.dbi.put(txn, key(doc._id), value(doc), PutFlags.MDB_NOOVERWRITE)
        case WriteOp.Upsert(doc) => store.instance.dbi.put(txn, key(doc._id), value(doc))
        case WriteOp.Delete(id) => store.instance.dbi.delete(txn, key(id))
      }
      .count
      .guarantee {
        // TODO: Consider rollback / abort if there's an error
        Task {
          txn.commit()
          txn.close()
        }
      }
  }.flatten.unit

  override protected def directGet[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = if (index == store.idField) {
    Task(Option(store.instance.dbi.get(readTxn, key(value.asInstanceOf[Id[Doc]]))).flatMap(b2d))
  } else {
    throw new UnsupportedOperationException(s"LMDBStore can only get on _id, but ${index.name} was attempted")
  }

  override def jsonStream: rapid.Stream[Json] =
    rapid.Stream.fromIterator(Task(new LMDBValueIterator(store.instance.dbi, readTxn))).map(b2j)

  override protected def _truncate: Task[Int] = count.flatTap { _ =>
    val txn = store.instance.env.txnWrite()
    try {
      store.instance.dbi.drop(txn)
      Task.unit
    } finally {
      txn.commit()
      txn.close()
    }
  }

  override protected def _count: Task[Int] = Task {
    store.instance.dbi.stat(readTxn).entries.toInt
  }

  /*override def _exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    val cursor = instance.dbi.openCursor(transaction)
    try {
      cursor.get(key(id), GetOp.MDB_SET_KEY)
    } finally {
      cursor.close()
    }
  }*/

  override protected def _commit: Task[Unit] = Task {
    _readTxn.close()
    _readTxn = env.txnRead()
  }.next(super._commit)

  override protected def _close: Task[Unit] = Task(readTxn.close()).next(super._close)

  private def key(id: Id[Doc]): ByteBuffer = {
    val bb = LMDBStore.keyBufferPool.get(512)
    bb.put(id.bytes)
    bb.flip()
  }

  private def value(doc: Doc): ByteBuffer = {
    val json = doc.json(store.model.rw)
    val value = JsonFormatter.Compact(json)
    val bb = LMDBStore.valueBufferPool.get(value.length)
    bb.put(value.getBytes)
    bb.flip()
  }

  private def b2d(bb: ByteBuffer): Option[Doc] = b2j(bb) match {
    case Null => None
    case json => Some(json.as[Doc](store.model.rw))
  }

  private def b2j(bb: ByteBuffer): Json = {
    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes)
    val jsonString = new String(bytes, "UTF-8")
    if (jsonString.isEmpty) {
      Null
    } else {
      JsonParser(jsonString)
    }
  }
}
