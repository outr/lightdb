package lightdb

import fabric.rw._
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{Store, StoreManager, StoreMode}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.util.Initializable

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean

/**
 * The database to be implemented. Collections *may* be used without a LightDB instance, but with drastically diminished
 * functionality. It is always ideal for collections to be associated with a database.
 */
trait LightDB extends Initializable {
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
   * True if this database has been disposed.
   */
  def disposed: Boolean = _disposed.get()

  /**
   * Backing key/value store used for persistent internal settings, StoredValues, and general key/value storage.
   */
  val backingStore: Collection[KeyValue, KeyValue.type] = collection(KeyValue, name = Some("_backingStore"))

  override protected def initialize(): Unit = {
    scribe.info(s"$name database initializing...")
    collections.foreach(_.init())
    // Truncate the database before we do anything if specified
    if (truncateOnInit) truncate()
    // Determine if this is an uninitialized database
    val dbInitialized = databaseInitialized.get()
    // Get applied database upgrades
    val applied = appliedUpgrades.get()
    // Determine upgrades that need to be applied
    val upgrades = this.upgrades.filter(u => u.alwaysRun || !applied.contains(u.label)) match {
      case list if !dbInitialized => list.filter(_.applyToNew)
      case list => list
    }
    if (upgrades.nonEmpty) {
      scribe.info(s"Applying ${upgrades.length} upgrades (${upgrades.map(_.label).mkString(", ")})...")
      doUpgrades(upgrades, stillBlocking = true)
    }
    // Set initialized
    databaseInitialized.set(true)
  }

  /**
   * Create a new Collection and associate it with this database. It is preferable that all collections be created
   * before the database is initialized, but collections that are added after init will automatically be initialized
   * during this method call.
   *
   * @param model the model to use for this collection
   * @param name the collection's name (defaults to None meaning it will be generated based on the model name)
   * @param store specify the store. If this is not set, the database's storeManager will be used to create one
   * @param maxInsertBatch the maximum number of inserts to include in a batch. Defaults to 1 million.
   * @param cacheQueries whether to cache queries in memory. This improves performance when running the same queries
   *                     with different parameters fairly drastically, but consumes a lot of memory if many queries are
   *                     executed in a single transaction.
   */
  def collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                   name: Option[String] = None,
                                                   store: Option[Store[Doc, Model]] = None,
                                                   maxInsertBatch: Int = 1_000_000,
                                                   cacheQueries: Boolean = false): Collection[Doc, Model] = {
    val n = name.getOrElse(model.getClass.getSimpleName.replace("$", ""))
    val s = store.getOrElse(storeManager.create[Doc, Model](this, n, StoreMode.All))
    val c = Collection[Doc, Model](n, model, s, maxInsertBatch, cacheQueries)
    synchronized {
      _collections = c :: _collections
    }
    if (isInitialized) {    // Already initialized database, init collection immediately
      c.init()
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

  def truncate(): Unit = collections.foreach { c =>
    val collection = c.asInstanceOf[Collection[KeyValue, KeyValue.type]]
    collection.transaction { implicit transaction =>
      collection.truncate()(transaction)
    }
  }

  private def doUpgrades(upgrades: List[DatabaseUpgrade],
                         stillBlocking: Boolean): Unit = upgrades.headOption match {
    case Some(upgrade) =>
      val continueBlocking = upgrades.exists(_.blockStartup)
      upgrade.upgrade(this)
      val applied = appliedUpgrades.get()
      appliedUpgrades.set(applied + upgrade.label)
      if (stillBlocking && !continueBlocking) {
        scribe.Platform.executionContext.execute(new Runnable {
          override def run(): Unit = doUpgrades(upgrades.tail, continueBlocking)
        })
      } else {
        doUpgrades(upgrades.tail, continueBlocking)
      }
    case None => scribe.info("Upgrades completed successfully")
  }

  def dispose(): Unit = if (_disposed.compareAndSet(false, true)) {
    collections.map(_.asInstanceOf[Collection[KeyValue, KeyValue.type]]).foreach { collection =>
      collection.dispose()
    }
  }
}
