package lightdb.lmdb

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import fabric.{Json, Null}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.store.{BufferedWritingTransaction, WriteOp}
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import org.lmdbjava.Txn
import rapid._

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

case class LMDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: LMDBStore[Doc, Model],
                                                                              instance: LMDBInstance,
                                                                              parent: Option[Transaction[Doc, Model]])
  extends BufferedWritingTransaction[Doc, Model]
    with PrefixScanningTransaction[Doc, Model] {
  private var _readTxn: Txn[ByteBuffer] = instance.newRead()

  def readTxn: Txn[ByteBuffer] = _readTxn

  private def d2b(doc: Doc): Array[Byte] = JsonFormatter
    .Compact(doc.json(store.model.rw))
    .getBytes(StandardCharsets.UTF_8)

  private def bytes2Doc(bytes: Array[Byte]): Doc = bytes2Json(bytes).as[Doc](store.model.rw)

  private def bytes2Json(bytes: Array[Byte]): Json = {
    val jsonString = new String(bytes, StandardCharsets.UTF_8)
    JsonParser(jsonString)
  }

  override protected def flushBuffer(stream: rapid.Stream[WriteOp[Doc]]): Task[Unit] = Task {
    store.instance.withWrite { txn =>
      stream
        .map {
          case WriteOp.Insert(doc) => instance.put(txn, doc._id.bytes, d2b(doc), overwrite = false)
          case WriteOp.Upsert(doc) => instance.put(txn, doc._id.bytes, d2b(doc), overwrite = true)
          case WriteOp.Delete(id) => instance.delete(txn, id.bytes)
        }
        .count
    }
  }.flatten.unit

  override protected def directGet[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = if index == store.idField then {
    Task(instance.get(readTxn, value.asInstanceOf[Id[Doc]].bytes).map(bytes2Doc))
  } else {
    throw new UnsupportedOperationException(s"LMDBStore can only get on _id, but ${index.name} was attempted")
  }

  override def jsonStream: rapid.Stream[Json] =
    rapid.Stream.fromIterator(Task(new LMDBValueIterator(store.instance.dbi, readTxn))).map(b2j)

  override def jsonPrefixStream(prefix: String): rapid.Stream[Json] =
    rapid.Stream.fromIterator(Task(new LMDBValueIterator(store.instance.dbi, readTxn, Some(prefix)))).map(b2j)

  override protected def _truncate: Task[Int] = count.flatTap { _ =>
    store.instance.withWrite { txn =>
      Task(store.instance.dbi.drop(txn))
    }
  }

  override protected def _count: Task[Int] = Task {
    store.instance.dbi.stat(readTxn).entries.toInt
  }

  /*override def _exists(id: Id[Doc])(transaction: Transaction[Doc]): Task[Boolean] = Task {
    val cursor = instance.dbi.openCursor(transaction)
    try {
      cursor.get(key(id), GetOp.MDB_SET_KEY)
    } finally {
      cursor.close()
    }
  }*/

  override protected def _commit: Task[Unit] = super._commit.next(Task {
    _readTxn.close()
    _readTxn = instance.newRead()
  })

  override protected def _close: Task[Unit] = Task(readTxn.close()).next(super._close)

  private def b2d(bb: ByteBuffer): Option[Doc] = b2j(bb) match {
    case Null => None
    case json => Some(json.as[Doc](store.model.rw))
  }

  private def b2j(byteBuffer: ByteBuffer): Json = {
    val bb = byteBuffer.duplicate()
    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes)
    val jsonString = new String(bytes, StandardCharsets.UTF_8)
    if jsonString.isEmpty then {
      Null
    } else {
      JsonParser(jsonString)
    }
  }
}

object LMDBTransaction {
  private val pool = new ByteBufferPool(512)
}