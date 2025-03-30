package lightdb.lmdb

import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.aggregate.AggregateQuery
import lightdb.{Id, LightDB, Query, SearchResults}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.materialized.MaterializedAggregate
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.{Transaction, TransactionKey}
import org.lmdbjava._
import rapid.{Task, Unique}

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

class LMDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                   model: Model,
                                                                   instance: LMDBInstance,
                                                                   val storeMode: StoreMode[Doc, Model],
                                                                   db: LightDB,
                                                                   storeManager: StoreManager) extends Store[Doc, Model](name, model, db, storeManager) {
  private val id = Unique()
  private val transactionKey: TransactionKey[LMDBTransaction] = TransactionKey(id)

  private lazy val dbi: Dbi[ByteBuffer] = instance.get(name)

  override protected def initialize(): Task[Unit] = super.initialize().next(Task {
    dbi
  })

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task {
    transaction.put(
      key = transactionKey,
      value = LMDBTransaction(instance)
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

  private def withWrite[Return](f: Txn[ByteBuffer] => Task[Return]): Task[Return] =
    instance.transactionManager.withWrite(f)

  override protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = withWrite { txn =>
    Task {
      dbi.put(txn, key(doc._id), value(doc), PutFlags.MDB_NOOVERWRITE)
      doc
    }
  }

  override protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = withWrite { txn =>
    Task {
      dbi.put(txn, key(doc._id), value(doc))
      doc
    }
  }

  override def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] =
    instance.transactionManager.exists(dbi, key(id))

  private def b2d(bb: ByteBuffer): Doc = b2j(bb).as[Doc](model.rw)

  private def b2j(bb: ByteBuffer): Json = {
    val bytes = new Array[Byte](bb.remaining())
    bb.get(bytes)
    val jsonString = new String(bytes, "UTF-8")
    JsonParser(jsonString)
  }

  override protected def _get[V](field: Field.UniqueIndex[Doc, V], value: V)
                     (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = if (field == idField) {
    instance.transactionManager.get(dbi, key(value.asInstanceOf[Id[Doc]])).map(_.map(b2d))
  } else {
    throw new UnsupportedOperationException(s"LMDBStore can only get on _id, but ${field.name} was attempted")
  }

  override protected def _delete[V](field: Field.UniqueIndex[Doc, V], value: V)
                        (implicit transaction: Transaction[Doc]): Task[Boolean] = withWrite { txn =>
    Task {
      if (field == idField) {
        dbi.delete(txn, key(value.asInstanceOf[Id[Doc]]))
      } else {
        throw new UnsupportedOperationException(s"LMDBStore can only get on _id, but ${field.name} was attempted")
      }
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] =
    instance.transactionManager.count(dbi)

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] =
    rapid.Stream.fromIterator(instance.transactionManager.withReadIterator(txn => new LMDBValueIterator(dbi, txn).map(b2j)))

  override def doSearch[V](query: Query[Doc, Model, V])
                          (implicit transaction: Transaction[Doc]): Task[SearchResults[Doc, Model, V]] =
    throw new UnsupportedOperationException("LMDBStore does not support searching")

  override def aggregate(query: AggregateQuery[Doc, Model])
                        (implicit transaction: Transaction[Doc]): rapid.Stream[MaterializedAggregate[Doc, Model]] =
    throw new UnsupportedOperationException("LMDBStore does not support aggregation")

  override def aggregateCount(query: AggregateQuery[Doc, Model])(implicit transaction: Transaction[Doc]): Task[Int] =
    throw new UnsupportedOperationException("LMDBStore does not support aggregation")

  override def truncate()(implicit transaction: Transaction[Doc]): Task[Int] = count.flatTap { _ =>
    withWrite { txn =>
      Task(dbi.drop(txn))
    }
  }

  override protected def doDispose(): Task[Unit] = super.doDispose().map(_ => instance.release(name))
}

object LMDBStore extends StoreManager {
  /**
   * Maximum number of collections. Defaults to 1,000
   */
  var MaxDbs: Int = 1_000

  /**
   * Map Size. Defaults to 100gig
   */
  var MapSize: Long = 100L * 1024 * 1024 * 1024

  /**
   * Max Readers. Defaults to 128
   */
  var MaxReaders: Int = 128

  private val keyBufferPool = new ByteBufferPool(512)
  private val valueBufferPool = new ByteBufferPool(512)

  private var instances = Map.empty[LightDB, LMDBInstance]

  def instance(db: LightDB): LMDBInstance = synchronized {
    instances.get(db) match {
      case Some(instance) => instance
      case None =>
        val directory = db.directory.get
        if (!Files.exists(directory)) {
          Files.createDirectories(directory)
        }
        val env = Env
          .create()
          .setMaxDbs(MaxDbs)
          .setMapSize(MapSize)
          .setMaxReaders(MaxReaders)
          .open(directory.toFile)
        val instance = LMDBInstance(env)
        instances += db -> instance
        instance
    }
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         storeMode: StoreMode[Doc, Model]): Store[Doc, Model] = {
    new LMDBStore[Doc, Model](
      name = name,
      model = model,
      instance = instance(db),
      storeMode = storeMode,
      db = db,
      storeManager = this
    )
  }
}
