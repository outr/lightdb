package lightdb.store

import fabric.Json
import fabric.define.DefType
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.{DocNotFoundException, ModelMissingFieldsException}
import lightdb.field.Field
import lightdb.field.Field._
import lightdb.lock.LockManager
import lightdb.transaction.Transaction
import lightdb.trigger.StoreTriggers
import lightdb.util.{Disposable, Initializable}
import lightdb.{Id, LightDB}
import rapid._

import java.io.File
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val name: String,
                                                                        val path: Option[Path],
                                                                        val model: Model,
                                                                        val lightDB: LightDB,
                                                                        val storeManager: StoreManager) extends Initializable with Disposable {
  def supportsArbitraryQuery: Boolean = false

  protected def id(doc: Doc): Id[Doc] = doc._id

  lazy val idField: UniqueIndex[Doc, Id[Doc]] = model._id

  lazy val lock: LockManager[Id[Doc], Doc] = new LockManager

  object trigger extends StoreTriggers[Doc]

  override protected def initialize(): Task[Unit] = Task.defer {
    model match {
      case jc: JsonConversion[_] =>
        val fieldNames = model.fields.map(_.name).toSet
        val missing = jc.rw.definition match {
          case DefType.Obj(map, _) => map.keys.filterNot { fieldName =>
            fieldNames.contains(fieldName)
          }.toList
          case DefType.Poly(values, _) =>
            values.values.flatMap(_.asInstanceOf[DefType.Obj].map.keys).filterNot { fieldName =>
              fieldNames.contains(fieldName)
            }.toList.distinct
          case _ => Nil
        }
        if (missing.nonEmpty) {
          throw ModelMissingFieldsException(name, missing)
        }
      case _ => // Can't do validation
    }

    // Give the Model a chance to initialize
    model.initialize(this).flatMap { _ =>
      // Verify the data is in-sync
      verify()
    }
  }.unit

  def storeMode: StoreMode[Doc, Model]

  lazy val fields: List[Field[Doc, _]] = if (storeMode.isIndexes) {
    model.fields.filter(_.isInstanceOf[Indexed[_, _]])
  } else {
    model.fields
  }

  lazy val t: Transactionless[Doc, Model] = Transactionless(this)

  protected def toString(doc: Doc): String = JsonFormatter.Compact(doc.json(model.rw))

  protected def fromString(string: String): Doc = toJson(string).as[Doc](model.rw)

  protected def toJson(string: String): Json = JsonParser(string)

  lazy val hasSpatial: Task[Boolean] = Task(fields.exists(_.isSpatial)).singleton

  def prepareTransaction(transaction: Transaction[Doc]): Task[Unit]

  def releaseTransaction(transaction: Transaction[Doc]): Task[Unit] = Task {
    transaction.commit()
  }

  protected def _insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc]

  protected def _upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc]

  def exists(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean]

  protected def _get[V](index: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Option[Doc]]

  def count(implicit transaction: Transaction[Doc]): Task[Int]

  protected def _delete[V](index: UniqueIndex[Doc, V], value: V)(implicit transaction: Transaction[Doc]): Task[Boolean]

  final def insert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = trigger.insert(doc).flatMap { _ =>
    _insert(doc)
  }

  final def upsert(doc: Doc)(implicit transaction: Transaction[Doc]): Task[Doc] = trigger.upsert(doc).flatMap { _ =>
    _upsert(doc)
  }

  final def insert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Task[Seq[Doc]] = for {
    _ <- docs.map(trigger.insert).tasks
    _ <- docs.map(insert).tasks
  } yield docs

  final def upsert(docs: Seq[Doc])(implicit transaction: Transaction[Doc]): Task[Seq[Doc]] = for {
    _ <- docs.map(trigger.upsert).tasks
    _ <- docs.map(upsert).tasks
  } yield docs

  def modify(id: Id[Doc],
             establishLock: Boolean = true,
             deleteOnNone: Boolean = false)
            (f: Forge[Option[Doc], Option[Doc]])
            (implicit transaction: Transaction[Doc]): Task[Option[Doc]] = {
    lock(id, get(id), establishLock) { existing =>
      f(existing).flatMap {
        case Some(doc) => upsert(doc).map(_ => Some(doc))
        case None if deleteOnNone => delete(id).map(_ => None)
        case None => Task.pure(None)
      }
    }
  }

  def apply[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Task[Doc] =
    get[V](f).map {
      case Some(doc) => doc
      case None =>
        val (field, value) = f(model)
        throw DocNotFoundException(name, field.name, value)
    }

  def apply(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Doc] = get(id).map(_.getOrElse {
    throw DocNotFoundException(name, "_id", id)
  })

  def get[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = {
    val (field, value) = f(model)
    _get(field, value)
  }

  def get(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Option[Doc]] = _get(idField, id)

  def getAll(ids: Seq[Id[Doc]])(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = rapid.Stream
    .emits(ids)
    .evalMap(apply)

  def getOrCreate(id: Id[Doc], create: => Doc, establishLock: Boolean = true)
                 (implicit transaction: Transaction[Doc]): Task[Doc] = modify(id, establishLock = establishLock) {
    case Some(doc) => Task.pure(Some(doc))
    case None => Task.pure(Some(create))
  }.map(_.get)

  def delete[V](f: Model => (UniqueIndex[Doc, V], V))(implicit transaction: Transaction[Doc]): Task[Boolean] = {
    val (field, value) = f(model)
    trigger.delete(field, value).flatMap(_ => _delete(field, value))
  }

  def delete(id: Id[Doc])(implicit transaction: Transaction[Doc]): Task[Boolean] = delete(_._id -> id)

  def list()(implicit transaction: Transaction[Doc]): Task[List[Doc]] = stream.toList

  def stream(implicit transaction: Transaction[Doc]): rapid.Stream[Doc] = jsonStream.map(_.as[Doc](model.rw))

  def jsonStream(implicit transaction: Transaction[Doc]): rapid.Stream[Json]

  def truncate()(implicit transaction: Transaction[Doc]): Task[Int]

  def verify(): Task[Boolean] = Task.pure(false)

  def reIndex(): Task[Boolean] = Task.pure(false)

  def reIndex(doc: Doc): Task[Boolean] = Task.pure(false)

  /**
   * Optimizes this store. This allows the implementation an opportunity to clean up, optimize, etc. to improve the
   * performance of the store.
   */
  def optimize(): Task[Unit] = Task.unit

  object transaction {
    private val set = ConcurrentHashMap.newKeySet[Transaction[Doc]]

    def active: Int = set.size()

    def apply[Return](f: Transaction[Doc] => Task[Return]): Task[Return] = create().flatMap { transaction =>
      f(transaction).guarantee(release(transaction))
    }

    def create(): Task[Transaction[Doc]] = for {
      _ <- logger.info(s"Creating new Transaction for $name").when(Store.LogTransactions)
      transaction = new Transaction[Doc]
      _ <- prepareTransaction(transaction)
      _ = set.add(transaction)
      _ <- trigger.transactionStart(transaction)
    } yield transaction

    def release(transaction: Transaction[Doc]): Task[Unit] = for {
      _ <- logger.info(s"Releasing Transaction for $name").when(Store.LogTransactions)
      _ <- trigger.transactionEnd(transaction)
      _ <- releaseTransaction(transaction)
      _ <- transaction.close()
      _ = set.remove(transaction)
    } yield ()

    def releaseAll(): Task[Int] = Task {
      val list = set.iterator().asScala.toList
      list.foreach { transaction =>
        release(transaction)
      }
      list.size
    }
  }

  override protected def doDispose(): Task[Unit] = transaction.releaseAll().flatMap { transactions =>
    logger.warn(s"Released $transactions active transactions").when(transactions > 0)
  }.guarantee(trigger.dispose())
}

object Store {
  var CacheQueries: Boolean = false
  var MaxInsertBatch: Int = 1_000_000
  var LogTransactions: Boolean = false

  def determineSize(file: File): Long = if (file.isDirectory) {
    file.listFiles().foldLeft(0L)((sum, file) => sum + determineSize(file))
  } else {
    file.length()
  }
}