package lightdb

import fabric.rw.RW
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.index.{IndexedCollection, Indexer}
import lightdb.store.StoreManager
import lightdb.util.Initializable
import lightdb.upgrade.DatabaseUpgrade
import scribe.{Level, Logger}

import java.nio.file.Path
import java.util.{Timer, TimerTask}
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.concurrent.duration._

trait LightDB extends Initializable {
  def directory: Option[Path]

  def storeManager: StoreManager

  /**
   * How frequently to run background updates. Defaults to every 30 seconds.
   */
  protected def updateFrequency: FiniteDuration = 30.seconds

  /**
   * If true, will automatically check the number of indexes vs the number of store entries and if there's a mismatch,
   * the index will be truncated and rebuilt at startup. Defaults to true.
   */
  protected def verifyIndexIntegrityOnStartup: Boolean = true

  /**
   * Disables extraneous logging from underlying implementations. Defaults to true.
   */
  protected def disableExtraneousLogging: Boolean = true

  /**
   * Automatically truncates all collections in the database during initialization if this is set to true.
   * Defaults to false.
   */
  protected def truncateOnInit: Boolean = false

  private val _disposed = new AtomicBoolean(false)
  private var _collections = List.empty[Collection[_, _]]

  val backingStore: Collection[KeyValue, KeyValue.type] = collection("_backingStore", KeyValue)

  protected lazy val databaseInitialized: StoredValue[Boolean] = stored[Boolean]("_databaseInitialized", false)
  protected lazy val appliedUpgrades: StoredValue[Set[String]] = stored[Set[String]]("_appliedUpgrades", Set.empty)

  def upgrades: List[DatabaseUpgrade]

  protected[lightdb] def verifyInitialized(): Unit = if (!isInitialized) throw new RuntimeException(s"Database not initialized!")

  override protected def initialize(): Unit = {
    scribe.info(s"LightDB initializing...")
    initLogging()
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
    // Verify integrity
    if (verifyIndexIntegrityOnStartup) {
      collections.foreach {
        case c: IndexedCollection[_, _] => c.indexer.maybeRebuild()
        case _ => ()
      }
    }
    // Set initialized
    databaseInitialized.set(true)
    // Start updater
    timer.schedule(updateTask, updateFrequency.toMillis, updateFrequency.toMillis)
  }

  private lazy val timer = new Timer

  private lazy val updateTask = new TimerTask {
    override def run(): Unit = update()
  }

  private def initLogging(): Unit = {
    if (disableExtraneousLogging) {
      Logger("com.oath.halodb").withMinimumLevel(Level.Warn).replace()
      Logger("org.apache.lucene.store").withMinimumLevel(Level.Warn).replace()
    }
  }

  def collections: List[Collection[_, _]] = _collections

  def collection[D <: Document[D], M <: DocumentModel[D]](name: String, model: M)
                                                         (implicit rw: RW[D]): Collection[D, M] = synchronized {
    val c = new Collection[D, M](name, model, this)
    _collections = c :: _collections
    c
  }

  def collection[D <: Document[D], M <: DocumentModel[D]](name: String,
                                                          model: M,
                                                          indexer: Indexer[D, M])
                                                         (implicit rw: RW[D]): IndexedCollection[D, M] = synchronized {
    val c = new IndexedCollection[D, M](name, model, indexer, this)
    model.listener += indexer
    _collections = c :: _collections
    c
  }

  def disposed: Boolean = _disposed.get()

  def truncate(): Unit = collections.foreach { c =>
    val collection = c.asInstanceOf[Collection[KeyValue, KeyValue.type]]
    collection.transaction { implicit transaction =>
      collection.truncate()
    }
  }

  def update(): Unit = collections.foreach(_.update())

  def dispose(): Unit = if (_disposed.compareAndSet(false, true)) {
    updateTask.cancel()
    collections.foreach(_.dispose())
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
}