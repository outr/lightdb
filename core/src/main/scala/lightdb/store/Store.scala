package lightdb.store

import fabric.define.DefType
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.ModelMissingFieldsException
import lightdb.field.Field
import lightdb.field.Field._
import lightdb.id.Id
import lightdb.lock.LockManager
import lightdb.progress.ProgressManager
import lightdb.transaction.Transaction
import lightdb.trigger.StoreTriggers
import lightdb.util.{Disposable, Initializable}
import rapid._

import java.io.File
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.Semaphore
import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class Store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](val name: String,
                                                                         val path: Option[Path],
                                                                         val model: Model,
                                                                         val lightDB: LightDB,
                                                                         val storeManager: StoreManager) extends Initializable with Disposable {
  type TX <: Transaction[Doc, Model]

  lazy val idField: UniqueIndex[Doc, Id[Doc]] = model._id

  lazy val lock: LockManager[Id[Doc], Doc] = new LockManager

  object trigger extends StoreTriggers[Doc, Model]

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

  lazy val hasSpatial: Task[Boolean] = Task(fields.exists(_.isSpatial)).singleton

  protected def createTransaction(parent: Option[Transaction[Doc, Model]]): Task[TX]

  private def releaseTransaction(transaction: TX): Task[Unit] = transaction.commit

  def verify(progressManager: ProgressManager = ProgressManager.none): Task[Boolean] = Task.pure(false)

  def reIndex(progressManager: ProgressManager = ProgressManager.none): Task[Boolean] = Task.pure(false)

  def reIndexDoc(doc: Doc): Task[Boolean] = Task.pure(false)

  /**
   * Optimizes this store. This allows the implementation an opportunity to clean up, optimize, etc. to improve the
   * performance of the store.
   */
  def optimize(): Task[Unit] = Task.unit

  /**
   * Whether the store can natively handle ExistsChild without generic resolution.
   * Defaults to false; backends can override to provide optimized handling.
   */
  def supportsNativeExistsChild: Boolean = false

  object transaction {
    private val set = ConcurrentHashMap.newKeySet[TX]
    private val sharedMap = new ConcurrentHashMap[String, Shared]()

    def active: Int = set.size()

    def apply[Return](f: TX => Task[Return]): Task[Return] = create(None).flatMap { transaction =>
      f(transaction).guarantee(release(transaction))
    }

    def shared[Return](name: String,
                       timeout: FiniteDuration,
                       parent: Option[Transaction[Doc, Model]] = None)
                      (f: TX => Task[Return]): Task[Return] = Task.defer {
      val s = sharedMap.computeIfAbsent(name, _ => {
        scribe.info(s"Creating Shared Transaction: $name")
        val tx = create(parent).sync()
        Shared(name, tx, timeout)
      })
      s.active.incrementAndGet()
      s.lastUsed.set(System.currentTimeMillis())
      s.withLock(f(s.tx)).guarantee(Task {
        s.active.decrementAndGet()
        s.lastUsed.set(System.currentTimeMillis())
      })
    }

    def create(parent: Option[Transaction[Doc, Model]]): Task[TX] = for {
      _ <- Task {
        if (!lightDB.isInitialized && !lightDB.isInitStarted) {
          throw new RuntimeException(s"Attempted to create a transaction for store '$name' before database initialization. Call db.init before using store.transaction(...).")
        }
      }
      _ <- logger.info(s"Creating new Transaction for $name").when(Store.LogTransactions)
      transaction <- createTransaction(parent)
      _ = set.add(transaction)
      _ <- trigger.transactionStart(transaction)
    } yield transaction

    def release(transaction: TX): Task[Unit] = for {
      _ <- logger.info(s"Releasing Transaction for $name").when(Store.LogTransactions)
      _ <- trigger.transactionEnd(transaction)
      _ <- releaseTransaction(transaction)
      _ <- transaction.close
      _ = set.remove(transaction)
    } yield ()

    def releaseAll(): Task[Int] = Task {
      val list = set.iterator().asScala.toList
      list.foreach { transaction =>
        release(transaction)
      }
      list.size
    }

    private case class Shared(name: String, tx: TX, timeout: FiniteDuration) {
      val active = new AtomicInteger(0)
      val lastUsed = new AtomicLong(0L)
      private val lock = new Semaphore(1)

      private val timeoutMillis = timeout.toMillis

      def withLock[A](task: Task[A]): Task[A] =
        Task(lock.acquire()).flatMap(_ => task.guarantee(Task(lock.release())))

      private def recurse(): Task[Unit] = Task.defer {
        val nextPossibleTimeout: Long = if (active.get() > 0) {
          timeoutMillis
        } else {
          (lastUsed.get() + timeoutMillis) - System.currentTimeMillis()
        }
        Task.sleep(nextPossibleTimeout.millis).next {
          if (active.get() == 0 && lastUsed.get() < System.currentTimeMillis() - timeoutMillis) {
            scribe.info(s"Releasing Shared Transaction: $name")
            sharedMap.remove(name)
            release(tx)
          } else {
            recurse()
          }
        }
      }

      recurse().start()
    }
  }

  override protected def doDispose(): Task[Unit] = transaction.releaseAll().flatMap { transactions =>
    logger.warn(s"Released $transactions active transactions").when(transactions > 0)
  }.guarantee(trigger.dispose).unit
}

object Store {
  var CacheQueries: Boolean = false
  var MaxInsertBatch: Int = 5_000
  var LogTransactions: Boolean = false

  def determineSize(file: File): Long = if (file.isDirectory) {
    file.listFiles().foldLeft(0L)((sum, file) => sum + determineSize(file))
  } else {
    file.length()
  }
}