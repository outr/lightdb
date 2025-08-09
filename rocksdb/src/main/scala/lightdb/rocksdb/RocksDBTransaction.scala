package lightdb.rocksdb

import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.rocksdb.RocksDBTransaction.writeOptions
import lightdb.transaction.{PrefixScanningTransaction, Transaction}
import org.rocksdb.{RocksIterator, WriteBatch, WriteOptions}
import rapid.Task

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

case class RocksDBTransaction[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: RocksDBStore[Doc, Model],
                                                                                 parent: Option[Transaction[Doc, Model]]) extends PrefixScanningTransaction[Doc, Model] { tx =>
  private var iterators = Set.empty[RocksIterator]
  private def rocksIterator: RocksIterator = {
    val i = store.handle match {
      case Some(h) => store.rocksDB.newIterator(h)
      case None => store.rocksDB.newIterator()
    }
    tx.synchronized {
      iterators += i
    }
    i
  }

  private def bytes2Doc(bytes: Array[Byte]): Doc = bytes2Json(bytes).as[Doc](store.model.rw)

  private def bytes2Json(bytes: Array[Byte]): Json = {
    val jsonString = new String(bytes, StandardCharsets.UTF_8)
    JsonParser(jsonString)
  }

  override def jsonPrefixStream(prefix: String): rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(iterator(rocksIterator, prefix = Some(prefix)))).map(bytes2Json)

  override def jsonStream: rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(iterator(rocksIterator))).map(bytes2Json)

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

  override def getAll(ids: Seq[Id[Doc]]): rapid.Stream[Doc] = rapid.Stream.force(Task {
    val keyBytes = ids.map(_.bytes).asJava
    val handle = store.handle.orNull

    val handleList = java.util.Collections.nCopies(ids.size, handle)

    rapid.Stream.fromIterator(Task {
      val rawResults = store.rocksDB.multiGetAsList(handleList, keyBytes)
      rawResults
        .asScala
        .iterator
        .filter(_ != null)
        .map(bytes2Doc)
    })
  })

  override protected def _insert(doc: Doc): Task[Doc] = _upsert(doc)

  override protected def _upsert(doc: Doc): Task[Doc] = Task {
    val json = doc.json(store.model.rw)
    val bytes = JsonFormatter.Compact(json).getBytes(StandardCharsets.UTF_8)
    store.handle match {
      case Some(h) => store.rocksDB.put(h, writeOptions, doc._id.bytes, bytes)
      case None => store.rocksDB.put(writeOptions, doc._id.bytes, bytes)
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
    iterator(rocksIterator).size
  }

  def estimatedCount: Task[Int] = Task {
    store.rocksDB.getLongProperty(store.handle.orNull, "rocksdb.estimate-num-keys").toInt
  }

  override protected def _delete[V](index: Field.UniqueIndex[Doc, V], value: V): Task[Boolean] = Task {
    val bytes = value.asInstanceOf[Id[Doc]].bytes
    store.handle match {
      case Some(h) => store.rocksDB.delete(h, writeOptions, bytes)
      case None => store.rocksDB.delete(writeOptions, bytes)
    }
    true
  }

  override protected def _commit: Task[Unit] = Task.unit

  override protected def _rollback: Task[Unit] = Task.unit

  override protected def _close: Task[Unit] = Task {
    iterators.foreach(_.close())
    iterators = Set.empty
  }

  override def truncate: Task[Int] = Task.defer {
    truncateManual
    // TODO: Revisit column family dropping - it's causing issues right now
    /*store.handle match {
      case Some(h) => count.map { size =>
        store.rocksDB.dropColumnFamily(h)
        store.resetHandle()
        size
      }
      case None => truncateManual
    }*/
  }

  private def truncateManual: Task[Int] = Task {
    val iter = rocksIterator
    val batch = new WriteBatch()
    var count = 0

    val delete = store.handle match {
      case Some(h) => (key: Array[Byte]) => batch.delete(h, key)
      case None => (key: Array[Byte]) => batch.delete(key)
    }

    iter.seekToFirst()
    while (iter.isValid) {
      delete(iter.key())
      count += 1
      iter.next()
    }
    store.rocksDB.write(writeOptions, batch)
    count
  }

  private def iterator(rocksIterator: RocksIterator,
                       value: Boolean = true,
                       prefix: Option[String] = None): Iterator[Array[Byte]] = {
    val iterator: Iterator[Array[Byte]] = new Iterator[Array[Byte]] {
      prefix match {
        case Some(s) => rocksIterator.seek(s.getBytes(StandardCharsets.UTF_8))     // Initialize the iterator to the prefix
        case None => rocksIterator.seekToFirst()                    // Seek to the provided value as the start position
      }

      val prefixBytes: Option[Array[Byte]] = prefix.map(_.getBytes(StandardCharsets.UTF_8))

      val isValid: () => Boolean = prefixBytes match {
        case Some(pBytes) =>
          () => rocksIterator.isValid && {
            val key = rocksIterator.key()
            key.length >= pBytes.length && java.util.Arrays.equals(key.take(pBytes.length), pBytes)
          }
        case None =>
          () => rocksIterator.isValid
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
    new ThreadConfinedBufferedIterator[Array[Byte]](iterator)
  }
}

object RocksDBTransaction {
  val writeOptions: WriteOptions = new WriteOptions()
    .setSync(false)
    .setDisableWAL(false)
}