package lightdb.store

import fabric.define.DefType
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.ModelMissingFieldsException
import lightdb.field.Field
import lightdb.field.Field.*
import lightdb.id.Id
import lightdb.lock.LockManager
import lightdb.progress.ProgressManager
import lightdb.transaction.{Transaction, WriteHandler}
import lightdb.transaction.batch.BatchConfig
import lightdb.transaction.handler.{AsyncWriteHandler, BufferedWriteHandler, DirectWriteHandler, QueuedWriteHandler}
import lightdb.store.write.WriteOp
import lightdb.trigger.StoreTriggers
import lightdb.util.{Disposable, Initializable, StoreMetrics}
import rapid.*

import java.io.File
import java.nio.file.Path
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, Semaphore}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}
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
    scribe.info(s"Initializing $name (${storeManager.name})...")
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
        if missing.nonEmpty then {
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

  lazy val fields: List[Field[Doc, _]] = if storeMode.isIndexes then {
    model.fields.filter(_.isInstanceOf[Indexed[_, _]])
  } else {
    model.fields
  }

  lazy val hasSpatial: Task[Boolean] = Task(fields.exists(_.isSpatial)).singleton

  protected def createTransaction(parent: Option[Transaction[Doc, Model]],
                                  batchConfig: BatchConfig,
                                  writeHandlerFactory: Transaction[Doc, Model] => WriteHandler[Doc, Model]): Task[TX]

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

  /**
   * Whether this store supports Filter.Nested query functionality.
   *
   * Stores that do not support nested queries should keep this false so Query can fail fast with
   * a deterministic unsupported-operation error.
   */
  def nestedQueryCapability: NestedQueryCapability = NestedQueryCapability.Unsupported

  def supportsNestedQueries: Boolean = nestedQueryCapability.supportsNestedQueries

  def supportsNativeNestedQueries: Boolean = nestedQueryCapability.isNative

  /**
   * Default batching behavior for transactions. Stores can override for optimal performance.
   */
  def defaultBatchConfig: BatchConfig = BatchConfig.Direct

  private val set = ConcurrentHashMap.newKeySet[TX]
  private val sharedMap = new ConcurrentHashMap[String, Shared]()

  lazy val transaction = TransactionBuilder(None, defaultBatchConfig)

  case class TransactionBuilder(parent: Option[Transaction[Doc, Model]], batchConfig: BatchConfig) {
    def withParent(parent: Transaction[Doc, Model]): TransactionBuilder = copy(parent = Some(parent))

    def withBatch(config: BatchConfig): TransactionBuilder = copy(batchConfig = config)

    def withBufferedBatch(maxBufferSize: Int = 20_000): TransactionBuilder =
      withBatch(BatchConfig.Buffered(maxBufferSize))

    def withQueuedBatch(maxQueueSize: Int = 5_000): TransactionBuilder =
      withBatch(BatchConfig.Queued(maxQueueSize))

    def withAsyncBatch(activeThreads: Int = 4,
                       chunkSize: Int = 5_000,
                       waitTime: FiniteDuration = 250.millis,
                       maxQueueSize: Int = 20_000): TransactionBuilder =
      withBatch(BatchConfig.Async(activeThreads = activeThreads,
                                  chunkSize = chunkSize,
                                  waitTime = waitTime,
                                  maxQueueSize = maxQueueSize))

    def withStoreNativeBatch: TransactionBuilder = withBatch(BatchConfig.StoreNative)

    def withDirectBatch: TransactionBuilder = withBatch(BatchConfig.Direct)

    def active: Int = set.size()

    def apply[Return](f: TX => Task[Return]): Task[Return] = create().flatMap { transaction =>
      f(transaction).guarantee(release(transaction))
    }

    def shared[Return](name: String,
                       timeout: FiniteDuration)
                      (f: TX => Task[Return]): Task[Return] = Task.defer {
      val s = sharedMap.computeIfAbsent(name, _ => {
        scribe.info(s"Creating Shared Transaction: $name")
        Shared(name, timeout, () => create(), tx => release(tx))
      })
      s.init().flatMap(_ => s.withLock(f))
    }

    def create(): Task[TX] = for
      _ <- Task {
        if !lightDB.isInitialized && !lightDB.isInitStarted then {
          throw new RuntimeException(s"Attempted to create a transaction for store '$name' before database initialization. Call db.init before using store.transaction(...).")
        }
      }
      _ <- logger.info(s"Creating new Transaction for $name").when(Store.LogTransactions)
      transaction <- createTransaction(parent, batchConfig, tx => createWriteHandler(tx, batchConfig))
      _ = set.add(transaction)
      _ <- trigger.transactionStart(transaction)
    yield transaction

    def release(transaction: TX): Task[Unit] = for
      _ <- trigger.transactionEnd(transaction)
      _ <- releaseTransaction(transaction)
      _ <- transaction.close
      _ = set.remove(transaction)
      _ <- logger.info(s"Released Transaction for $name").when(Store.LogTransactions)
    yield ()

    def releaseAll(): Task[Int] = Task.defer {
      val list = set.iterator().asScala.toList
      if list.isEmpty then Task.pure(0)
      else list.map(tx => release(tx).attempt).tasks.map { results =>
        results.foreach {
          case scala.util.Failure(t) => scribe.warn(s"Error releasing transaction during dispose of $name: ${t.getMessage}")
          case _ =>
        }
        list.size
      }
    }
  }

  protected def flushOps(transaction: Transaction[Doc, Model], ops: Seq[WriteOp[Doc]]): Task[Unit] =
    transaction.applyWriteOps(ops)

  protected def createWriteHandler(transaction: Transaction[Doc, Model], config: BatchConfig): WriteHandler[Doc, Model] =
    config match {
      case BatchConfig.Direct =>
        new DirectWriteHandler(
          doc => transaction.applyWriteOps(Seq(WriteOp.Insert(doc))).map(_ => doc),
          doc => transaction.applyWriteOps(Seq(WriteOp.Upsert(doc))).map(_ => doc),
          id => transaction.applyWriteOps(Seq(WriteOp.Delete(id))).map(_ => true)
        )
      case BatchConfig.Buffered(maxBufferSize) =>
        new BufferedWriteHandler(maxBufferSize, ops => flushOps(transaction, ops))
      case BatchConfig.Queued(maxQueueSize) =>
        new QueuedWriteHandler(maxQueueSize, ops => flushOps(transaction, ops))
      case BatchConfig.Async(activeThreads, chunkSize, waitTime, maxQueueSize) =>
        new AsyncWriteHandler(activeThreads, chunkSize, waitTime, maxQueueSize, ops => flushOps(transaction, ops))
      case BatchConfig.StoreNative =>
        createNativeWriteHandler(transaction)
    }

  protected def createNativeWriteHandler(transaction: Transaction[Doc, Model]): WriteHandler[Doc, Model] =
    new DirectWriteHandler(
      doc => transaction.applyWriteOps(Seq(WriteOp.Insert(doc))).map(_ => doc),
      doc => transaction.applyWriteOps(Seq(WriteOp.Upsert(doc))).map(_ => doc),
      id => transaction.applyWriteOps(Seq(WriteOp.Delete(id))).map(_ => true)
    )

  protected def maximumConcurrency: Int = 1_000
  protected def defaultSharedTransactions: Int = 1

  protected case class Shared(name: String,
                              timeout: FiniteDuration,
                              createTx: () => Task[TX],
                              releaseTx: TX => Task[Unit]) { shared =>
    private val active = new AtomicInteger(0)
    private val lastUsed = new AtomicLong(0L)
    @volatile private var started = false
    private val initStarted = new java.util.concurrent.atomic.AtomicBoolean(false)
    private val initLatch = new java.util.concurrent.CountDownLatch(1)
    private val maxTransactions = math.max(1, maximumConcurrency)
    private val initialTransactions = math.min(math.max(1, defaultSharedTransactions), maxTransactions)
    private val mutex = new Semaphore(maxTransactions, true)
    private val pool = new LinkedBlockingQueue[TX]()
    private val total = new AtomicInteger(0)

    private val timeoutMillis = timeout.toMillis

    def init(): Task[Unit] = Task.defer {
      if initStarted.compareAndSet(false, true) then {
        List.fill(initialTransactions)(createTx()).tasks.flatMap { txs =>
          Task {
            txs.foreach(pool.offer)
            total.set(txs.size)
            initLatch.countDown()
          }
        }
      } else {
        Task {
          initLatch.await()
        }
      }
    }

    private def acquireTx: Task[TX] = Task.defer {
      val tx = pool.poll()
      if tx != null then Task.pure(tx)
      else {
        val current = total.get()
        if current < maxTransactions && total.compareAndSet(current, current + 1) then {
          createTx()
        } else {
          Task(pool.take())
        }
      }
    }

    private def releaseToPool(tx: TX): Task[Unit] = Task(pool.offer(tx)).unit

    def withLock[A](task: TX => Task[A]): Task[A] = Task.defer {
      active.incrementAndGet()
      if !started then {
        shared.synchronized {
          if !started then {
            started = true
            recurse().start()
          }
        }
      }
      Task.defer {
        val waitStart = System.nanoTime()
        Task(mutex.acquire())
          .map(_ => StoreMetrics.recordSharedWait(System.nanoTime() - waitStart))
          .next {
            acquireTx.flatMap { tx =>
              task(tx).guarantee(releaseToPool(tx).guarantee(Task(mutex.release())))
            }
          }
          .guarantee(Task {
            active.decrementAndGet()
            lastUsed.set(System.currentTimeMillis())
          })
      }
    }

    private def recurse(): Task[Unit] = Task.defer {
      val nextPossibleTimeout: Long = if active.get() > 0 then {
        timeoutMillis
      } else {
        math.max(0L, (lastUsed.get() + timeoutMillis) - System.currentTimeMillis())
      }
      Task.sleep(nextPossibleTimeout.millis).next {
        if active.get() == 0 && lastUsed.get() < System.currentTimeMillis() - timeoutMillis then {
          scribe.info(s"Releasing Shared Transaction: $name")
          sharedMap.remove(name)
          val drained = Task {
            val list = scala.collection.mutable.ListBuffer.empty[TX]
            var next = pool.poll()
            while next != null do {
              list += next
              next = pool.poll()
            }
            total.set(0)
            list.toList
          }
          drained.flatMap(_.map(releaseTx).tasks.unit)
        } else {
          recurse()
        }
      }
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

  def determineSize(file: File): Long = if file.isDirectory then {
    file.listFiles().foldLeft(0L)((sum, file) => sum + determineSize(file))
  } else {
    file.length()
  }
}