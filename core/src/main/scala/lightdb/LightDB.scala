package lightdb

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.{catsSyntaxApplicativeByName, catsSyntaxParallelSequence1, toTraverseOps}
import fabric.rw._
import lightdb.model.{AbstractCollection, Collection, DocumentModel}
import lightdb.upgrade.DatabaseUpgrade

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import scribe.cats.{io => logger}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

abstract class LightDB {
  def directory: Path

  protected def updateFrequency: FiniteDuration = 30.seconds

  private val _initialized = new AtomicBoolean(false)
  private val _disposed = new AtomicBoolean(false)

  private var stores = List.empty[Store]

  protected lazy val backingStore: Collection[KeyValue] = Collection[KeyValue]("_backingStore", this)
  protected lazy val databaseInitialized: StoredValue[Boolean] = stored[Boolean]("_databaseInitialized", false)
  protected lazy val appliedUpgrades: StoredValue[Set[String]] = stored[Set[String]]("_appliedUpgrades", Set.empty)

  def initialized: Boolean = _initialized.get()
  def disposed: Boolean = _disposed.get()

  def collections: List[AbstractCollection[_]]
  def upgrades: List[DatabaseUpgrade]

  def commit(): IO[Unit] = collections.map(_.commit()).sequence.map(_ => ())

  protected[lightdb] def verifyInitialized(): Unit = if (!initialized) throw new RuntimeException(s"Database not initialized ($directory)!")

  def init(truncate: Boolean = false): IO[Unit] = if (_initialized.compareAndSet(false, true)) {
    for {
      _ <- logger.info(s"LightDB initializing ${directory.getFileName.toString} collection...")
      // Truncate the database before we do anything if specified
      _ <- this.truncate().whenA(truncate)
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
      // Set initialized
      _ <- databaseInitialized.set(true)
    } yield {
      // Start updater
      recursiveUpdates().unsafeRunAndForget()
    }
  } else {
    IO.unit
  }

  private def recursiveUpdates(): IO[Unit] = for {
    _ <- IO.sleep(updateFrequency)
    _ <- update().recoverWith {
      case t: Throwable => logger.error(s"Update process threw an error. Continuing...", t)
    }.whenA(!disposed)
    _ <- recursiveUpdates().whenA(!disposed)
  } yield ()

  protected[lightdb] def createStoreInternal(name: String): Store = synchronized {
    verifyInitialized()
    val store = createStore(name)
    stores = store :: stores
    store
  }

  protected def collection[D <: Document[D]](name: String,
                                             model: DocumentModel[D],
                                             defaultCommitMode: CommitMode = CommitMode.Manual,
                                             atomic: Boolean = true)
                                            (implicit rw: RW[D]): AbstractCollection[D] = AbstractCollection[D](
    name = name,
    db = this,
    model = model,
    defaultCommitMode = defaultCommitMode,
    atomic = atomic
  )

  protected def createStore(name: String): Store

  def truncate(): IO[Unit] = collections.map(_.truncate()).parSequence.map(_ => ())

  def update(): IO[Unit] = collections.map(_.update()).sequence.map(_ => ())

  def dispose(): IO[Unit] = for {
    _ <- collections.map(_.dispose()).parSequence
    _ <- stores.map(_.dispose()).parSequence
    _ = _disposed.set(true)
  } yield ()

  protected object stored {
    def apply[T](key: String,
                 default: => T,
                 cache: Boolean = true,
                 collection: Collection[KeyValue] = backingStore)
                (implicit rw: RW[T]): StoredValue[T] = StoredValue[T](key, collection, () => default, cache = cache)

    def opt[T](key: String,
               cache: Boolean = true,
               collection: Collection[KeyValue] = backingStore)
              (implicit rw: RW[T]): StoredValue[Option[T]] = StoredValue[Option[T]](key, collection, () => None, cache = cache)
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