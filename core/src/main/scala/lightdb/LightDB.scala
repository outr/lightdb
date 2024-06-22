package lightdb

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeByName, catsSyntaxParallelSequence1, toTraverseOps}
import fabric.rw.RW
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.index.{IndexedCollection, Indexer}
import lightdb.store.StoreManager
import lightdb.util.Initializable
import lightdb.upgrade.DatabaseUpgrade
import scribe.{Level, Logger}
import scribe.cats.{io => logger}

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.util.Try

trait LightDB extends Initializable {
  def directory: Path

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

  private val _disposed = new AtomicBoolean(false)
  private var _collections = List.empty[Collection[_, _]]

  val backingStore: Collection[KeyValue, KeyValue.type] = collection("_backingStore", KeyValue)

  protected lazy val databaseInitialized: StoredValue[Boolean] = stored[Boolean]("_databaseInitialized", false)
  protected lazy val appliedUpgrades: StoredValue[Set[String]] = stored[Set[String]]("_appliedUpgrades", Set.empty)

  def upgrades: List[DatabaseUpgrade]

  protected[lightdb] def verifyInitialized(): Unit = if (!isInitialized) throw new RuntimeException(s"Database not initialized!")

  override protected def initialize(): IO[Unit] = {
    for {
      _ <- logger.info(s"LightDB initializing...")
      _ = initLogging()
      _ <- collections.map(_.init()).parSequence
      // Truncate the database before we do anything if specified
      // TODO: Can't pass truncate in as an arg here anymore
      //      _ <- this.truncate().whenA(truncate)
      // Determine if this is an uninitialized database
      dbInitialized <- databaseInitialized.get()
      // Get applied database upgrades
      applied <- appliedUpgrades.get()
      // Determine upgrades that need to be applied
      upgrades = this.upgrades.filter(u => u.alwaysRun || !applied.contains(u.label)) match {
        case list if !dbInitialized => list.filter(_.applyToNew)
        case list => list
      }
      _ <- (for {
        _ <- logger.info(s"Applying ${upgrades.length} upgrades (${upgrades.map(_.label).mkString(", ")})...")
        _ <- doUpgrades(upgrades, stillBlocking = true)
      } yield ()).whenA(upgrades.nonEmpty)
      // Verify integrity
      // TODO: Revisit
      /*_ <- collections.map { collection =>
        collection.model match {
          case indexSupport: IndexSupport[_] => for {
            storeCount <- collection.size
            indexCount <- indexSupport.index.size
            _ <- logger.warn(s"Index and Store out of sync for ${collection.collectionName} (Store: $storeCount, Index: $indexCount). Rebuilding index...").whenA(storeCount != indexCount)
            _ <- collection.reIndex().whenA(storeCount != indexCount)
          } yield ()
          case _ => IO.unit
        }
      }.sequence.whenA(verifyIndexIntegrityOnStartup)*/
      // Set initialized
      _ <- databaseInitialized.set(true)
    } yield {
      // Start updater
      recursiveUpdates().unsafeRunAndForget()(cats.effect.unsafe.implicits.global)
    }
  }

  private def initLogging(): Unit = {
    if (disableExtraneousLogging) {
      Logger("com.oath.halodb").withMinimumLevel(Level.Warn).replace()
      Logger("org.apache.lucene.store").withMinimumLevel(Level.Warn).replace()
    }
    Logger.system.installJUL()
  }

  def storeManager: StoreManager

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

  private def recursiveUpdates(): IO[Unit] = for {
    _ <- IO.sleep(updateFrequency)
    _ <- update().recoverWith {
      case t: Throwable => logger.error(s"Update process threw an error. Continuing...", t)
    }.whenA(!disposed)
    _ <- recursiveUpdates().whenA(!disposed)
  } yield ()

  def truncate(): IO[Unit] = collections.map { c =>
    val collection = c.asInstanceOf[Collection[KeyValue, KeyValue.type]]
    collection.transaction { implicit transaction =>
      collection.truncate()
    }
  }.parSequence.map(_ => ())

  def update(): IO[Unit] = collections.map(_.update()).sequence.map(_ => ())

  def dispose(): IO[Unit] = if (_disposed.compareAndSet(false, true)) {
    for {
      _ <- IO.unit // TODO: wait for active transactions to close
      _ <- collections.map(_.dispose()).parSequence
      _ <- IO.unit // TODO: //stores.map(_.dispose()).parSequence
    } yield ()
  } else {
    IO.unit
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
                         stillBlocking: Boolean): IO[Unit] = upgrades.headOption match {
    case Some(upgrade) =>
      val continueBlocking = upgrades.exists(_.blockStartup)
      val io = for {
        _ <- upgrade.upgrade(this)
        applied <- appliedUpgrades.get()
        _ <- appliedUpgrades.set(applied + upgrade.label)
        _ <- doUpgrades(upgrades.tail, continueBlocking)
      } yield ()
      if (stillBlocking && !continueBlocking) {
        io.unsafeRunAndForget()(cats.effect.unsafe.IORuntime.global)
        IO.unit
      } else {
        io
      }
    case None => logger.info("Upgrades completed successfully")
  }
}