package lightdb

import fabric.rw._
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.feature.{DBFeatureKey, FeatureSupport}
import lightdb.store.{StoreManager, StoreMode}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.util.{Disposable, Initializable}
import rapid._
import scribe.{rapid => logger}

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.{Failure, Success}

/**
 * The database to be implemented. Collections *may* be used without a LightDB instance, but with drastically diminished
 * functionality. It is always ideal for collections to be associated with a database.
 */
trait LightDB extends Initializable with Disposable with FeatureSupport[DBFeatureKey] {
  /**
   * Identifiable name for this database. Defaults to using the class name.
   */
  def name: String = getClass.getSimpleName.replace("$", "")

  /**
   * The base directory for this database. If None, the database is expected to operate entirely in memory.
   */
  def directory: Option[Path]

  /**
   * Default StoreManager to use for collections that do not specify a Store.
   */
  def storeManager: StoreManager

  /**
   * List of upgrades that should be applied at the start of this database.
   */
  def upgrades: List[DatabaseUpgrade]

  /**
   * Automatically truncates all collections in the database during initialization if this is set to true.
   * Defaults to false.
   */
  protected def truncateOnInit: Boolean = false

  protected lazy val databaseInitialized: StoredValue[Boolean] = stored[Boolean]("_databaseInitialized", false)
  protected lazy val appliedUpgrades: StoredValue[Set[String]] = stored[Set[String]]("_appliedUpgrades", Set.empty)

  private var _collections = List.empty[Collection[_, _]]
  private val _disposed = new AtomicBoolean(false)

  /**
   * All collections registered with this database
   */
  def collections: List[Collection[_, _]] = _collections

  /**
   * Returns a list of matching collection names based on the provided names
   */
  def collectionsByNames(collectionNames: String*): List[Collection[_, _]] = {
    val set = collectionNames.toSet
    collections.filter(c => set.contains(c.name))
  }

  /**
   * Offers each collection the ability to re-index data if supported. Only stores that separate storage and indexing
   * (like SplitStore) will do any work. Returns the number of stores that were re-indexed. Provide the list of the
   * collections to re-index or all collections will be invoked.
   */
  def reIndex(collections: List[Collection[_, _]] = collections): Task[Int] = collections.map(_.reIndex()).tasks.map(_.count(identity))

  /**
   * Offers each collection the ability to optimize the store.
   */
  def optimize(collections: List[Collection[_, _]] = collections): Task[Unit] = collections.map(_.store.optimize()).tasks.unit

  /**
   * True if this database has been disposed.
   */
  def disposed: Boolean = _disposed.get()

  /**
   * Backing key/value store used for persistent internal settings, StoredValues, and general key/value storage.
   */
  lazy val backingStore: Collection[KeyValue, KeyValue.type] = collection(KeyValue, name = Some("_backingStore"))

  override protected def initialize(): Task[Unit] = for {
    _ <- logger.info(s"$name database initializing...")
    _ = backingStore
    _ <- collections.map(_.init).tasks
    // Truncate the database before we do anything if specified
    _ <- truncate().when(truncateOnInit)
    // Determine if this is an uninitialized database
    dbInitialized <- databaseInitialized.get()
    // Get applied database upgrades
    applied <- appliedUpgrades.get()
    // Determine upgrades that need to be applied
    // TODO: Test upgrades that run asynchronously!
    upgrades = this.upgrades.filter(u => u.alwaysRun || !applied.contains(u.label))
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
   * Create a new Collection and associate it with this database. It is preferable that all collections be created
   * before the database is initialized, but collections that are added after init will automatically be initialized
   * during this method call.
   *
   * Note: If both are specified, store takes priority over storeManager.
   *
   * @param model          the model to use for this collection
   * @param name           the collection's name (defaults to None meaning it will be generated based on the model name)
   * @param storeManager   specify the StoreManager. If this is not set, the database's storeManager will be used.
   */
  def collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                                    name: Option[String] = None,
                                                                    storeManager: Option[StoreManager] = None): Collection[Doc, Model] = {
    val n = name.getOrElse(model.getClass.getSimpleName.replace("$", ""))
    val store = storeManager.getOrElse(this.storeManager).create[Doc, Model](this, model, n, StoreMode.All())
    val c = Collection[Doc, Model](n, model, store)
    synchronized {
      _collections = _collections ::: List(c)
    }
    if (isInitialized) { // Already initialized database, init collection immediately
      c.init.sync()
    }
    c
  }

  object stored {
    def apply[T](key: String,
                 default: => T,
                 persistence: Persistence = Persistence.Stored,
                 collection: Collection[KeyValue, KeyValue.type] = backingStore)
                (implicit rw: RW[T]): StoredValue[T] = StoredValue[T](
      key = key,
      collection = collection,
      default = () => default,
      persistence = persistence
    )

    def opt[T](key: String,
               persistence: Persistence = Persistence.Stored,
               collection: Collection[KeyValue, KeyValue.type] = backingStore)
              (implicit rw: RW[T]): StoredValue[Option[T]] = StoredValue[Option[T]](
      key = key,
      collection = collection,
      default = () => None,
      persistence = persistence
    )
  }

  def truncate(): Task[Unit] = collections.map { c =>
    val collection = c.asInstanceOf[Collection[KeyValue, KeyValue.type]]
    collection.transaction { implicit transaction =>
      collection.truncate()(transaction)
    }
  }.tasks.unit

  private def doUpgrades(upgrades: List[DatabaseUpgrade],
                         dbInitialized: Boolean,
                         stillBlocking: Boolean): Task[Unit] = upgrades.headOption match {
    case Some(upgrade) =>
      val runUpgrade = dbInitialized || upgrade.applyToNew
      val continueBlocking = upgrades.exists(u => u.blockStartup && (dbInitialized || u.applyToNew))

      val task = upgrade
        .upgrade(this)
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
    collections.map(_.asInstanceOf[Collection[KeyValue, KeyValue.type]]).map { collection =>
      collection.dispose
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