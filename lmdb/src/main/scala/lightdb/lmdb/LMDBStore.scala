package lightdb.lmdb

import fabric.{Json, Null}
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.doc.{Document, DocumentModel}
import lightdb.field.Field
import lightdb.store.{BufferedWritingStore, Store, StoreManager, StoreMode, WriteBuffer, WriteOp}
import lightdb.transaction.{Transaction, TransactionKey}
import lightdb.{Id, LightDB}
import org.lmdbjava._
import rapid._

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import scala.language.implicitConversions

class LMDBStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                   path: Option[Path],
                                                                   model: Model,
                                                                   instance: LMDBInstance,
                                                                   val storeMode: StoreMode[Doc, Model],
                                                                   db: LightDB,
                                                                   storeManager: StoreManager) extends Store[Doc, Model](name, path, model, db, storeManager) with BufferedWritingStore[Doc, Model] {
  private val id = Unique()
  private val transactionKey: TransactionKey[LMDBTransaction[Doc]] = TransactionKey(id)

  protected implicit def transaction2Txn(transaction: Transaction[Doc]): Txn[ByteBuffer] =
    transaction(transactionKey).readTxn

  override def prepareTransaction(transaction: Transaction[Doc]): Task[Unit] = Task {
    transaction.put(
      key = transactionKey,
      value = LMDBTransaction[Doc](instance.env)
    )
  }.flatMap(_ => super.prepareTransaction(transaction))

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

  private def b2d(bb: ByteBuffer): Option[Doc] = b2j(bb) match {
    case Null => None
    case json => Some(json.as[Doc](model.rw))
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

  override protected def getWriteBuffer(transaction: Transaction[Doc]): WriteBuffer[Doc] =
    transaction(transactionKey).writeBuffer

  override protected def setWriteBuffer(writeBuffer: WriteBuffer[Doc], transaction: Transaction[Doc]): Unit =
    transaction(transactionKey).writeBuffer = writeBuffer

  override protected def _truncate(implicit transaction: Transaction[Doc]): Task[Int] = count.flatTap { _ =>
    val txn = instance.env.txnWrite()
    try {
      instance.dbi.drop(txn)
      Task.unit
    } finally {
      txn.commit()
      txn.close()
    }
  }

  override protected def _get[V](field: Field.UniqueIndex[Doc, V], value: V)
                                (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = if (field == idField) {
    Task(Option(instance.dbi.get(transaction, key(value.asInstanceOf[Id[Doc]]))).flatMap(b2d))
  } else {
    throw new UnsupportedOperationException(s"LMDBStore can only get on _id, but ${field.name} was attempted")
  }

  override def _exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = Task {
    val cursor = instance.dbi.openCursor(transaction)
    try {
      cursor.get(key(id), GetOp.MDB_SET_KEY)
    } finally {
      cursor.close()
    }
  }

  override def count(implicit transaction: Transaction[Doc]): Task[Int] = Task {
    instance.dbi.stat(transaction).entries.toInt
  }

  override def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json] =
    rapid.Stream.fromIterator(Task(new LMDBValueIterator(instance.dbi, transaction))).map(b2j)

  override protected def flushBuffer(stream: rapid.Stream[WriteOp[Doc]]): Task[Unit] = Task {
    val txn = instance.env.txnWrite()
    stream
      .map {
        case WriteOp.Insert(doc) => instance.dbi.put(txn, key(doc._id), value(doc), PutFlags.MDB_NOOVERWRITE)
        case WriteOp.Upsert(doc) => instance.dbi.put(txn, key(doc._id), value(doc))
        case WriteOp.Delete(id) => instance.dbi.delete(txn, key(id))
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

  override protected def doDispose(): Task[Unit] = super.doDispose().map { _ =>
    instance.env.sync(true)
    instance.dbi.close()
    instance.env.close()
  }
}

object LMDBStore extends StoreManager {
  override type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = LMDBStore[Doc, Model]

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

  def instance(db: LightDB, path: Path): LMDBInstance = synchronized {
    instances.get(db) match {
      case Some(instance) => instance
      case None =>
        val instance = createInstance(path)
        instances += db -> instance
        instance
    }
  }

  private def createInstance(path: Path): LMDBInstance = {
    if (!Files.exists(path)) {
      Files.createDirectories(path)
    }
    val env = Env
      .create()
      .setMaxDbs(MaxDbs)
      .setMapSize(MapSize)
      .setMaxReaders(MaxReaders)
      .open(path.toFile, EnvFlags.MDB_WRITEMAP)
    LMDBInstance(env, env.openDbi(name, DbiFlags.MDB_CREATE))
  }

  override def create[Doc <: Document[Doc], Model <: DocumentModel[Doc]](db: LightDB,
                                                                         model: Model,
                                                                         name: String,
                                                                         path: Option[Path],
                                                                         storeMode: StoreMode[Doc, Model]): S[Doc, Model] = {
    new LMDBStore[Doc, Model](
      name = name,
      path = path,
      model = model,
      instance = createInstance(path.get),
      storeMode = storeMode,
      db = db,
      storeManager = this
    )
  }
}
