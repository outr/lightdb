package lightdb.rocksdb

import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb._
import lightdb.aggregate.AggregateQuery
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field._
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, DBOptions, FlushOptions, Options, RocksDB, RocksIterator}
import rapid.Task

import java.nio.file.{Files, Path}
import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

class RocksDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                      model: Model,
                                                                      rocksDB: RocksDB,
                                                                      sharedStore: Option[RocksDBSharedStoreInstance],
                                                                      val storeMode: StoreMode[Doc, Model],
                                                                      lightDB: LightDB,
                                                                      storeManager: StoreManager) extends Store[Doc, Model](name, model, lightDB, storeManager) {
  private val handle: Option[ColumnFamilyHandle] = sharedStore.map { ss =>
    ss.existingHandle match {
      case Some(handle) => handle
      case None => rocksDB.createColumnFamily(new ColumnFamilyDescriptor(ss.handle.getBytes("UTF-8")))
    }
  }

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task.unit

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = upsert(doc)

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    val json = doc.json(model.rw)
    val bytes = JsonFormatter.Compact(json).getBytes("UTF-8")
    handle match {
      case Some(h) => rocksDB.put(h, doc._id.bytes, bytes)
      case None => rocksDB.put(doc._id.bytes, bytes)
    }
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    handle match {
      case Some(h) => rocksDB.keyExists(h, id.bytes)
      case None => rocksDB.keyExists(id.bytes)
    }
  }

  override protected def _get[V](field: UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      val bytes = value.asInstanceOf[Id[Doc]].bytes
      Option(handle match {
        case Some(h) => rocksDB.get(h, bytes)
        case None => rocksDB.get(bytes)
      }).map(bytes2Doc)
    } else {
      throw new UnsupportedOperationException(s"RocksDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  private def bytes2Doc(bytes: Array[Byte]): Doc = bytes2Json(bytes).as[Doc](model.rw)

  private def bytes2Json(bytes: Array[Byte]): Json = {
    val jsonString = new String(bytes, "UTF-8")
    JsonParser(jsonString)
  }

  override protected def _delete[V](field: UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    val bytes = value.asInstanceOf[Id[Doc]].bytes
    handle match {
      case Some(h) => rocksDB.delete(h, bytes)
      case None => rocksDB.delete(bytes)
    }
    true
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    iterator(handle match {
      case Some(h) => rocksDB.newIterator(h)
      case None => rocksDB.newIterator()
    }).size
  }

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] = rapid.Stream
    .fromIterator(Task(iterator {
      handle match {
        case Some(h) => rocksDB.newIterator(h)
        case None => rocksDB.newIterator()
      }
    })).map(bytes2Json)

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    (handle match {
      case Some(h) => iterator(rocksDB.newIterator(h), value = false).map(a => rocksDB.delete(h, a))
      case None => iterator(rocksDB.newIterator(), value = false).map(rocksDB.delete)
    }).size
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().next(Task {
    val o = new FlushOptions
    handle match {
      case Some(h) =>
        rocksDB.flush(o, h)
      case None =>
        rocksDB.flush(o)
        rocksDB.closeE()
    }
  })

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
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = RocksDBStore[Doc, Model]

  def createRocksDB(directory: Path): (RocksDB, List[ColumnFamilyHandle]) = {
    RocksDB.loadLibrary()

    Files.createDirectories(directory.getParent)
    val path = directory.toAbsolutePath.toString
    val columnFamilies = new util.ArrayList[ColumnFamilyDescriptor]
    columnFamilies.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY))
    RocksDB.listColumnFamilies(new Options(), path)
      .asScala
      .foreach { name =>
        columnFamilies.add(new ColumnFamilyDescriptor(name))
      }
    val handles = new util.ArrayList[ColumnFamilyHandle]()
    val options = new DBOptions()
      .setCreateIfMissing(true)
    RocksDB.open(options, path, columnFamilies, handles) -> handles.asScala.toList
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): RocksDBStore[Doc, Model] =
    new RocksDBStore[Doc, Model](
      name = name,
      model = model,
      rocksDB = createRocksDB(db.directory.get.resolve(name))._1,
      sharedStore = None,
      storeMode = storeMode,
      lightDB = db,
      storeManager = this
    )
}
