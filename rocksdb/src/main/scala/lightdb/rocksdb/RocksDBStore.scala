package lightdb.rocksdb

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.aggregate.AggregateQuery
import lightdb.collection.Collection
import lightdb._
import lightdb.field.Field._
import lightdb.doc.{Document, DocumentModel}
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Conversion, Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.rocksdb.{FlushOptions, Options, RocksDB, RocksIterator}

import java.nio.file.{Files, Path}

class RocksDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](directory: Path, val storeMode: StoreMode) extends Store[Doc, Model] {
  private lazy val db: RocksDB = {
    Files.createDirectories(directory.getParent)
    val options = new Options()
      .setCreateIfMissing(true)
    RocksDB.open(options, directory.toAbsolutePath.toString)
  }

  override def init(collection: Collection[Doc, Model]): Unit = {
    super.init(collection)

    RocksDB.loadLibrary()
    db
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Unit = ()

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = upsert(doc)

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Unit = {
    val json = doc.json(collection.model.rw)
    db.put(doc._id.bytes, JsonFormatter.Compact(json).getBytes("UTF-8"))
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Boolean = db.keyExists(id.bytes)

  override def get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Option[Doc] = {
    if (field == idField) {
      Option(db.get(value.asInstanceOf[Id[Doc]].bytes)).map(bytes2Doc)
    } else {
      throw new UnsupportedOperationException(s"RocksDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  private def bytes2Doc(bytes: Array[Byte]): Doc = {
    val jsonString = new String(bytes, "UTF-8")
    val json = JsonParser(jsonString)
    json.as[Doc](collection.model.rw)
  }

  override def delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Boolean = {
    db.delete(value.asInstanceOf[Id[Doc]].bytes)
    true
  }

  override def count(implicit transaction: Transaction[Doc]): Int = iterator(db.newIterator()).size

  override def iterator(implicit transaction: Transaction[Doc]): Iterator[Doc] = iterator(db.newIterator())
    .map(bytes2Doc)

  override def doSearch[V](query: Query[Doc, Model],
                           conversion: Conversion[Doc, V])
                          (implicit transaction: Transaction[Doc]): SearchResults[Doc, Model, V] =
    throw new UnsupportedOperationException("RocksDBStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): Iterator[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("RocksDBStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Int =
    throw new UnsupportedOperationException("RocksDBStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Int = iterator(db.newIterator(), value = false)
    .map(db.delete)
    .size

  override def dispose(): Unit = {
    db.flush(new FlushOptions)
    db.close()
  }

  private def iteratorOld(rocksIterator: RocksIterator, value: Boolean = true): Iterator[Array[Byte]] = new Iterator[Array[Byte]] {
    override def hasNext: Boolean = rocksIterator.isValid

    override def next(): Array[Byte] = try {
      if (value) {
        rocksIterator.value()
      } else {
        rocksIterator.key()
      }
    } finally {
      rocksIterator.next()
    }
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
                                                                         name: String,
                                                                         storeMode: StoreMode): Store[Doc, Model] =
    new RocksDBStore[Doc, Model](db.directory.get.resolve(name), storeMode)
}