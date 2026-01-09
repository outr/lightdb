package lightdb

import fabric.rw._
import lightdb.doc.{Document, DocumentModel}
import lightdb.feature.{DBFeatureKey, FeatureSupport}
import lightdb.field.Field
import lightdb.graph.{EdgeDocument, EdgeModel, ReverseEdgeDocument}
import lightdb.progress.ProgressManager
import lightdb.store.multi.MultiStore
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.transaction.{Transaction, TransactionManager}
import lightdb.trigger.StoreTrigger
import lightdb.upgrade.DatabaseUpgrade
import lightdb.util.{Disposable, Initializable}
import profig.Profig
import rapid._

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Failure, Success}

/**
 * The database to be implemented. stores *may* be used without a LightDB instance, but with drastically diminished
 * functionality. It is always ideal for stores to be associated with a database.
 */
trait LightDB extends Initializable with Disposable with FeatureSupport[DBFeatureKey] {
  type SM <: StoreManager
  type S[Doc <: Document[Doc], Model <: DocumentModel[Doc]] = storeManager.S[Doc, Model]

  /**
   * Identifiable name for this database. Defaults to using the class name.
   */
  def name: String = getClass.getSimpleName.replace("$", "")

  /**
   * The base directory for this database. If None, the database is expected to operate entirely in memory.
   */
  def directory: Option[Path]

  /**
   * Default StoreManager to use for stores that do not specify a Store.
   */
  val storeManager: SM

  /**
   * List of upgrades that should be applied at the start of this database.
   */
  def upgrades: List[DatabaseUpgrade]

  /**
   * Automatically truncates all stores in the database during initialization if this is set to true.
   * Defaults to false.
   */
  protected def truncateOnInit: Boolean = false

  protected lazy val databaseInitialized: StoredValue[Boolean] = stored[Boolean]("_databaseInitialized", false)
  protected lazy val appliedUpgrades: StoredValue[Set[String]] = stored[Set[String]]("_appliedUpgrades", Set.empty)

  private var _stores = List.empty[Store[_, _ <: DocumentModel[_]]]
  private val _disposed = new AtomicBoolean(false)
  @volatile private var initStarted = false

  /**
   * True if this database's initialization has started (db.init has been invoked), even if it hasn't completed yet.
   *
   * This is useful for enforcing invariants like "no transactions before init", while still allowing LightDB's own
   * initialization process to transact as needed.
   */
  def isInitStarted: Boolean = initStarted

  override protected def beforeInitialize(): Unit = initStarted = true

  /**
   * All stores registered with this database
   */
  def stores: List[Store[_, _ <: DocumentModel[_]]] = _stores

  /**
   * Returns a list of matching store names based on the provided names
   */
  def storesByNames(storeNames: String*): List[Store[_, _]] = {
    val set = storeNames.toSet
    stores.filter(c => set.contains(c.name))
  }

  /**
   * Offers each store the ability to re-index data if supported. Only stores that separate storage and indexing
   * (like SplitStore) will do any work. Returns the number of stores that were re-indexed. Provide the list of the
   * stores to re-index or all stores will be invoked.
   */
  def reIndex(stores: List[Store[_, _]] = stores, progressManager: ProgressManager = ProgressManager.none): Task[Int] = if (stores.nonEmpty) {
    val pms = progressManager.split(stores.length)
    stores.zip(pms).map {
      case (store, pm) => store.reIndex(pm)
    }.tasksPar.map(_.count(identity))
  } else {
    Task.pure(0)
  }

  /**
   * Offers each store the ability to optimize the store.
   */
  def optimize(stores: List[Store[_, _]] = stores): Task[Unit] = stores.map(_.optimize()).tasks.unit

  /**
   * True if this database has been disposed.
   */
  def disposed: Boolean = _disposed.get()

  /**
   * Backing key/value store used for persistent internal settings, StoredValues, and general key/value storage.
   */
  lazy val backingStore: S[KeyValue, KeyValue.type] = store(KeyValue, name = Some("_backingStore"))

  lazy val transactions: TransactionManager = new TransactionManager

  override protected def initialize(): Task[Unit] = for {
    // NOTE: Profig initialization is owned by the application (or test bootstrap).
    _ <- logger.info(s"$name database initializing...")
    _ = backingStore
    _ <- logger.info(s"Initializing stores: ${stores.map(_.name).mkString(", ")}...")
    _ <- stores.map(_.init).tasks
    // Truncate the database before we do anything if specified
    _ <- truncate().next(logger.info("Truncating database...")).when(truncateOnInit)
    // Determine if this is an uninitialized database
    dbInitialized <- databaseInitialized.get()
    // Get applied database upgrades
    applied <- appliedUpgrades.get()
    // Determine upgrades that need to be applied
    allUpgrades = this.upgrades.filter(u => u.alwaysRun || !applied.contains(u.label))
    blockingUpgrades = allUpgrades.filter(_.blockStartup)
    asyncUpgrades = allUpgrades.filterNot(_.blockStartup)
    upgrades = blockingUpgrades ::: asyncUpgrades
    _ <- logger.info(s"Applying ${upgrades.length} upgrades (${upgrades.map(_.label).mkString(", ")})...")
      .when(upgrades.nonEmpty)
    _ <- doUpgrades(upgrades, dbInitialized = dbInitialized, stillBlocking = true).when(upgrades.nonEmpty)
    // Setup shutdown hook
    _ = Runtime.getRuntime.addShutdownHook(new Thread(() => {
      dispose.sync()
    }))
    // Set initialized
    _ <- databaseInitialized.set(true)
  } yield ()

  /**
   * Create a new store and associate it with this database. It is preferable that all stores be created
   * before the database is initialized, but stores that are added after init will automatically be initialized
   * during this method call.
   *
   * @param model the model to use for this store
   * @param name the store's name (defaults to None meaning it will be generated based on the model name)
   */
  def store[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                               name: Option[String] = None): storeManager.S[Doc, Model] = {
    val n = name.getOrElse(model.modelName)
    val path = directory.map(_.resolve(n))
    val store = storeManager.create[Doc, Model](this, model, n, path, StoreMode.All())
    synchronized {
      _stores = _stores ::: List(store)
    }
    if (isInitialized) { // Already initialized database, init store immediately
      store.init.sync()
    }
    store
  }

  /**
   * Create a new store with an explicit StoreMode (advanced).
   *
   * This is useful for "search index + storage store" compositions where the searching store should be
   * `StoreMode.Indexes(storage)` so that doc materialization happens from storage.
   */
  def storeWithMode[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                                       storeMode: StoreMode[Doc, Model],
                                                                       name: Option[String] = None): storeManager.S[Doc, Model] = {
    val n = name.getOrElse(model.getClass.getSimpleName.replace("$", ""))
    val path = directory.map(_.resolve(n))
    val store = storeManager.create[Doc, Model](this, model, n, path, storeMode)
    synchronized {
      _stores = _stores ::: List(store)
    }
    if (isInitialized) { // Already initialized database, init store immediately
      store.init.sync()
    }
    store
  }

  /**
   * Create a new store and associate it with this database. It is preferable that all stores be created
   * before the database is initialized, but stores that are added after init will automatically be initialized
   * during this method call.
   *
   * @param model the model to use for this store
   * @param name the store's name (defaults to None meaning it will be generated based on the model name)
   * @param storeManager specify the StoreManager
   */
  def storeCustom[Doc <: Document[Doc], Model <: DocumentModel[Doc], SM <: StoreManager](model: Model,
                                                                                         storeManager: SM,
                                                                                         name: Option[String] = None): storeManager.S[Doc, Model] = {
    val n = name.getOrElse(model.getClass.getSimpleName.replace("$", ""))
    val path = directory.map(_.resolve(n))
    val store = storeManager.create[Doc, Model](this, model, n, path, StoreMode.All())
    synchronized {
      _stores = _stores ::: List(store)
    }
    if (isInitialized) { // Already initialized database, init store immediately
      store.init.sync()
    }
    store
  }

  /**
   * Create a new store with a specific StoreManager and explicit StoreMode (advanced).
   */
  def storeCustomWithMode[Doc <: Document[Doc], Model <: DocumentModel[Doc], SM <: StoreManager](model: Model,
                                                                                                 storeManager: SM,
                                                                                                 storeMode: StoreMode[Doc, Model],
                                                                                                 name: Option[String] = None): storeManager.S[Doc, Model] = {
    val n = name.getOrElse(model.getClass.getSimpleName.replace("$", ""))
    val path = directory.map(_.resolve(n))
    val store = storeManager.create[Doc, Model](this, model, n, path, storeMode)
    synchronized {
      _stores = _stores ::: List(store)
    }
    if (isInitialized) { // Already initialized database, init store immediately
      store.init.sync()
    }
    store
  }

  /**
   * Registers a pre-constructed store with this database.
   *
   * This exists to support advanced / specialized store construction paths that want to avoid going through
   * StoreManager.create (e.g. to keep call sites and internal implementation fully type-safe without casts).
   *
   * The store will be initialized immediately if the database is already initialized.
   */
  def registerStore[Doc <: Document[Doc], Model <: DocumentModel[Doc], S0 <: Store[Doc, Model]](store: S0): S0 = {
    synchronized {
      _stores = _stores ::: List(store)
    }
    if (isInitialized) {
      store.init.sync()
    }
    store
  }

  case class MultiStoreBuilder[
    Doc <: Document[Doc],
    Model <: DocumentModel[Doc],
    Txn <: Transaction[Doc, Model],
    S <: Store[Doc, Model] { type TX = Txn },
    SM <: StoreManager,
    Key
  ](model: Model,
    keys: List[Key],
    storeManager: SM,
    namePrefix: String,
    key2Name: Key => String
   ) {
    def withKeys(keys: Key*): MultiStoreBuilder[Doc, Model, Txn, S, SM, Key] =
      copy(keys = (this.keys ::: keys.toList).distinct)
    def withNamePrefix(prefix: String): MultiStoreBuilder[Doc, Model, Txn, S, SM, Key] =
      copy(namePrefix = prefix)
    def withStoreManager[CSM <: StoreManager](storeManager: CSM): MultiStoreBuilder[Doc, Model, Txn, S, CSM, Key] =
      copy[Doc, Model, Txn, S, CSM, Key](storeManager = storeManager)
    def create(): MultiStore[Doc, Model, Txn, S, Key] = {
      val stores: Map[Key, S] = keys.map { key =>
        val storeName = s"${namePrefix}_${key2Name(key)}"
        val s = storeCustom[Doc, Model, SM](model, storeManager, name = Some(storeName)).asInstanceOf[S]
        key -> s
      }.toMap
      new MultiStore[Doc, Model, Txn, S, Key](stores)
    }
  }

  def multiStore[
    Doc <: Document[Doc],
    Model <: DocumentModel[Doc],
    Txn <: Transaction[Doc, Model],
    S <: Store[Doc, Model] { type TX = Txn },
    Key
  ](model: Model)(implicit key2Name: Key => String): MultiStoreBuilder[Doc, Model, Txn, S, SM, Key] =
    MultiStoreBuilder[Doc, Model, Txn, S, SM, Key](model, Nil, storeManager, model.modelName, key2Name)

  def reverseStore[
    E <: EdgeDocument[E, F, T],
    F <: Document[F],
    T <: Document[T],
    M <: DocumentModel[E],
    RM <: EdgeModel[ReverseEdgeDocument[E, F, T], T, F]](model: RM,
                                                         forward: S[E, M]): S[ReverseEdgeDocument[E, F, T], RM] = {
    val reverse = store[ReverseEdgeDocument[E, F, T], RM](model)
    forward.trigger += new StoreTrigger[E, M] {
      private val map = new ConcurrentHashMap[Transaction[E, M], reverse.TX]

      override def transactionStart(transaction: Transaction[E, M]): Task[Unit] = super
        .transactionStart(transaction)
        .flatMap { _ =>
          reverse.transaction.create(None).map { tx =>
            map.put(transaction, tx)
          }
        }

      override def transactionEnd(transaction: Transaction[E, M]): Task[Unit] = super
        .transactionEnd(transaction)
        .flatMap { _ =>
          val tx = map.remove(transaction)
          reverse.transaction.release(tx)
        }

      private def tx(transaction: Transaction[E, M]): reverse.TX = map.get(transaction)

      override def insert(doc: E, transaction: Transaction[E, M]): Task[Unit] = super
        .insert(doc, transaction)
        .flatTap { _ =>
          tx(transaction).insert(ReverseEdgeDocument[E, F, T](doc))
        }

      override def upsert(doc: E, transaction: Transaction[E, M]): Task[Unit] = super
        .upsert(doc, transaction)
        .flatTap { _ =>
          tx(transaction).upsert(ReverseEdgeDocument[E, F, T](doc))
        }

      override def delete[V](index: Field.UniqueIndex[E, V], value: V, transaction: Transaction[E, M]): Task[Unit] = transaction
        .get(_ => index -> value)
        .flatTap {
          case Some(doc) => tx(transaction).delete(ReverseEdgeDocument[E, F, T](doc)._id)
          case None => Task.unit
        }
        .next(super.delete(index, value, transaction))

      override def truncate: Task[Unit] = super.truncate.next {
        reverse.t.truncate.unit
      }
    }
    reverse
  }

  object stored {
    def apply[T](key: String,
                 default: => T,
                 persistence: Persistence = Persistence.Stored,
                 store: Store[KeyValue, KeyValue.type] = backingStore)
                (implicit rw: RW[T]): StoredValue[T] = StoredValue[T](
      key = key,
      store = store,
      default = () => default,
      persistence = persistence
    )

    def opt[T](key: String,
               persistence: Persistence = Persistence.Stored,
               store: Store[KeyValue, KeyValue.type] = backingStore)
              (implicit rw: RW[T]): StoredValue[Option[T]] = StoredValue[Option[T]](
      key = key,
      store = store,
      default = () => None,
      persistence = persistence
    )
  }

  def truncate(): Task[Unit] = stores.map { c =>
    val store = c.asInstanceOf[Store[KeyValue, KeyValue.type]]
    store.transaction(_.truncate)
  }.tasks.unit

  private def doUpgrades(upgrades: List[DatabaseUpgrade],
                         dbInitialized: Boolean,
                         stillBlocking: Boolean): Task[Unit] = upgrades.headOption match {
    case Some(upgrade) =>
      val runUpgrade = dbInitialized || upgrade.applyToNew
      val continueBlocking = upgrades.exists(u => u.blockStartup && (dbInitialized || u.applyToNew))

      val task = logger.info(s"Starting ${upgrade.label} database upgrade...")
        .next(upgrade.upgrade(this))
        .elapsed.flatMap {
          case (_, elapsed) => logger.info(s"Finished ${upgrade.label} database upgrade in $elapsed seconds")
        }
        .flatMap { _ =>
          appliedUpgrades.modify(_ + upgrade.label)
        }
        .when(runUpgrade)
        .attempt
        .flatMap[Unit] {
          case Success(_) => doUpgrades(upgrades.tail, dbInitialized, continueBlocking)
          case Failure(throwable) => logger
            .error(s"Database Upgrade: ${upgrade.label} failed", throwable)
            .map(_ => throw throwable)
        }
      if (stillBlocking && !continueBlocking) {
        task.start()
        Task.unit
      } else {
        task
      }
    case None => logger.info("Upgrades completed successfully")
  }

  override protected def doDispose(): Task[Unit] = if (_disposed.compareAndSet(false, true)) {
    stores.map(_.asInstanceOf[Store[KeyValue, KeyValue.type]]).map { store =>
      store.dispose
    }.tasks.flatMap { _ =>
      features.toList.map {
        case d: Disposable => d.dispose
        case _ => Task.unit // Ignore
      }.tasks
    }.unit
  } else {
    Task.unit
  }
}