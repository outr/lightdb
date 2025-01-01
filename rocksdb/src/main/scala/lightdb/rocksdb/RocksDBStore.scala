package lightdb.rocksdb

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.rocksdb.{FlushOptions, Options, RocksDB, RocksIterator}
import rapid.Task

import java.nio.file.{Files, Path}

class RocksDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                      model: Model,
                                                                      directory: Path,
                                                                      val storeMode: StoreMode[Doc, Model]) extends Store[Doc, Model](name, model) {
  RocksDB.loadLibrary()
  private val db: RocksDB = {
    Files.createDirectories(directory.getParent)
    val options = new Options()
      .setCreateIfMissing(true)
    RocksDB.open(options, directory.toAbsolutePath.toString)
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = upsert(doc)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    val json = doc.json(model.rw)
    db.put(doc._id.bytes, JsonFormatter.Compact(json).getBytes("UTF-8"))
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task(db.keyExists(id.bytes))

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      Option(db.get(value.asInstanceOf[Id[Doc]].bytes)).map(bytes2Doc)
    } else {
      throw new UnsupportedOperationException(s"RocksDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  private def bytes2Doc(bytes: Array[Byte]): Doc = {
    val jsonString = new String(bytes, "UTF-8")
    val json = JsonParser(jsonString)
    json.as[Doc](model.rw)
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    db.delete(value.asInstanceOf[Id[Doc]].bytes)
    true
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task(iterator(db.newIterator()).size)

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = rapid.Stream
    .fromIterator(Task(iterator(db.newIterator())))
    .map(bytes2Doc)

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] =
    throw new UnsupportedOperationException("RocksDBStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("RocksDBStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    throw new UnsupportedOperationException("RocksDBStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    iterator(db.newIterator(), value = false)
      .map(db.delete)
      .size
  }

  override def dispose(): Task[Unit] = Task {
    db.flush(new FlushOptions)
    db.close()
  }

  private def iterator(rocksIterator: RocksIterator, value: Boolean = true): Iterator[Array[Byte]] = new Iterator[Array[Byte]] {
    // Initialize the iterator to the first position
    rocksIterator.seekToFirst()

    override def hasNext: Boolean = rocksIterator.isValid

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

object RocksDBStore extends StoreManager {
  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] =
    new RocksDBStore[Doc, Model](name, model, db.directory.get.resolve(name), storeMode)
}