package lightdb.lmdb

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.aggregate.AggregateQuery
import lightdb.{Id, LightDB, Query, SearchResults}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.Transaction
import org.lmdbjava._
import rapid.Task

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

class LMDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                   model: Model,
                                                                   instance: LMDBInstance,
                                                                   val storeMode: StoreMode[Doc, Model]) extends Store[Doc, Model](name, model) {
  private lazy val dbi: Dbi[ByteBuffer] = instance.get(name)

  override protected def initialize(): Task[Unit] = Task(dbi)

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task {
    transaction.put(
      key = StateKey,
      value = instance.createTransaction()
    )
  }

  private def key(id: Id[Doc]): ByteBuffer = {
    val bb = LMDBStore.keyBufferPool.get(512)
    bb.put(id.bytes)
    bb.flip()
  }

  private def value(doc: Doc): ByteBuffer = {
    val json = doc.json(model.rw)
    val value = JsonFormatter.Compact(json)
    val bb = LMDBStore.valueBufferPool.get(value.length)
    bb.put(value.getBytes)
    bb.flip()
  }

  override def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    val key = this.key(doc._id)
    val value = this.value(doc)
    dbi.put(getState.txn, key, value, PutFlags.MDB_NOOVERWRITE)
    doc
  }

  override def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = Task {
    val key = this.key(doc._id)
    val value = this.value(doc)
    dbi.put(getState.txn, key, value)
    doc
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    val cursor = dbi.openCursor(getState.txn) // ✅ Open a cursor for efficient key lookup
    try {
      cursor.get(key(id), GetOp.MDB_SET_KEY)
    } finally {
      cursor.close()
    }
  }

  private def b2d(bb: ByteBuffer): Doc = {
    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes)
    val jsonString = new String(bytes, "UTF-8")
    val json = JsonParser(jsonString)
    json.as[Doc](model.rw)
  }

  override def get[V](field: Field.UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = Task {
    if (field == idField) {
      Option(dbi.get(getState.txn, key(value.asInstanceOf[Id[Doc]]))).filterNot(_.remaining() == 0).map(b2d)
    } else {
      throw new UnsupportedOperationException(s"LMDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def delete[V](field: Field.UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    if (field == idField) {
      dbi.delete(getState.txn, key(value.asInstanceOf[Id[Doc]]))
    } else {
      throw new UnsupportedOperationException(s"LMDBStore can only get on _id, but ${field.name} was attempted")
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    dbi.stat(getState.txn).entries.toInt
  }

  override def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] =
    rapid.Stream.fromIterator(Task(new LMDBValueIterator(dbi, getState.txn).map(b2d)))

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] =
    throw new UnsupportedOperationException("LMDBStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("LMDBStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    throw new UnsupportedOperationException("LMDBStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = count.flatTap { _ =>
    Task(dbi.drop(getState.txn))
  }

  override protected def doDispose(): Task[Unit] = instance.release(name)
}

object LMDBStore extends StoreManager {
  private val keyBufferPool = new ByteBufferPool(512)
  private val valueBufferPool = new ByteBufferPool(512)

  def createInstance(directory: Path,
                maxDbs: Int = 1_000,                          // 1,000 default
                mapSize: Long = 100L * 1024 * 1024 * 1024,    // 100 gig
                maxReaders: Int = 128): LMDBInstance = {
    if (!Files.exists(directory)) {
      Files.createDirectories(directory)
    }
    val env = Env
      .create()
      .setMaxDbs(maxDbs)
      .setMapSize(mapSize)
      .setMaxReaders(maxReaders)
      .open(directory.toFile)
    LMDBInstance(env)
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] = {
    new LMDBStore[Doc, Model](
      name = name,
      model = model,
      instance = createInstance(db.directory.get.resolve(name)),
      storeMode = storeMode
    )
  }
}