package lightdb.rocksdb

import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import lightdb.Id
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import org.rocksdb.RocksIterator
import rapid.Task

case class RocksDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: RocksDBStore[Doc, Model],
                                                                                 parent: Option[Transaction[Doc, Model]]) extends PrefixScanningTransaction[Doc, Model] {
  private def bytes2Doc(bytes: Array[Byte]): Doc = bytes2Json(bytes).as[Doc](store.model.rw)

  private def bytes2Json(bytes: Array[Byte]): Json = {
    val jsonString = new String(bytes, "UTF-8")
    JsonParser(jsonString)
  }

  override def jsonPrefixStream(prefix: String): rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(iterator({
      store.handle match {
        case Some(h) => store.rocksDB.newIterator(h)
        case None => store.rocksDB.newIterator()
      }
    }, prefix = Some(prefix)))).map(bytes2Json)

  override def jsonStream: rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(iterator {
      store.handle match {
        case Some(h) => store.rocksDB.newIterator(h)
        case None => store.rocksDB.newIterator()
      }
    })).map(bytes2Json)

  override protected def _get[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Option[Doc]] = Task {
    if (index == store.idField) {
      val bytes = value.asInstanceOf[Id[Doc]].bytes
      Option(store.handle match {
        case Some(h) => store.rocksDB.get(h, bytes)
        case None => store.rocksDB.get(bytes)
      }).map(bytes2Doc)
    } else {
      throw new UnsupportedOperationException(s"RocksDBStore can only get on _id, but ${index.name} was attempted")
    }
  }

  override protected def _insert(doc: Doc): Task[Doc] = _upsert(doc)

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    val json = doc.json(store.model.rw)
    val bytes = JsonFormatter.Compact(json).getBytes("UTF-8")
    store.handle match {
      case Some(h) => store.rocksDB.put(h, doc._id.bytes, bytes)
      case None => store.rocksDB.put(doc._id.bytes, bytes)
    }
    doc
  }

  override protected def _exists(id: Id[Doc]): Task[Boolean] = Task {
    store.handle match {
      case Some(h) => store.rocksDB.keyExists(h, id.bytes)
      case None => store.rocksDB.keyExists(id.bytes)
    }
  }

  override protected def _count: Task[Int] = Task {
    iterator(store.handle match {
      case Some(h) => store.rocksDB.newIterator(h)
      case None => store.rocksDB.newIterator()
    }).size
  }

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] = Task {
    val bytes = value.asInstanceOf[Id[Doc]].bytes
    store.handle match {
      case Some(h) => store.rocksDB.delete(h, bytes)
      case None => store.rocksDB.delete(bytes)
    }
    true
  }

  override protected def _commit: Task[Unit] = Task.unit

  override protected def _rollback: Task[Unit] = Task.unit

  override protected def _close: Task[Unit] = Task.unit

  override def truncate: Task[Int] = Task {
    (store.handle match {
      case Some(h) => iterator(store.rocksDB.newIterator(h), value = false).map(a => store.rocksDB.delete(h, a))
      case None => iterator(store.rocksDB.newIterator(), value = false).map(store.rocksDB.delete)
    }).size
  }

  private def iterator(rocksIterator: RocksIterator,
                       value: Boolean = true,
                       prefix: Option[String] = None): Iterator[Array[Byte]] = new Iterator[Array[Byte]] {
    prefix match {
      case Some(s) => rocksIterator.seek(s.getBytes("UTF-8"))     // Initialize the iterator to the prefix
      case None => rocksIterator.seekToFirst()                    // Seek to the provided value as the start position
    }

    val isValid: () => Boolean = prefix match {
      case Some(s) => () => rocksIterator.isValid && new String(rocksIterator.key(), "UTF-8").startsWith(s)
      case None => () => rocksIterator.isValid
    }

    override def hasNext: Boolean = isValid()

    override def next(): Array[Byte] = {
      if (!hasNext) throw new NoSuchElementException("No more elements in the RocksDB iterator")

      val result = if (value) {
        rocksIterator.value()
      } else {
        rocksIterator.key()
      }

      // Move to the next entry after retrieving the current value or key
      rocksIterator.next()
      result
    }
  }
}
