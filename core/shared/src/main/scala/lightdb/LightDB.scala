package lightdb

import cats.effect.IO
import cats.implicits.catsSyntaxApplicativeByName
import cats.instances.list._
import cats.syntax.parallel._
import fabric.rw.{Asable, Convertible, RW}
import lightdb.collection.Collection
import lightdb.index.Indexer
import lightdb.store.ObjectStore
import lightdb.upgrade.DatabaseUpgrade

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicBoolean
import scribe.cats.{io => logger}

abstract class LightDB(val directory: Option[Path]) {
  private val _initialized = new AtomicBoolean(false)

  private var _collections = List.empty[Collection[_]]

  protected val backingStore: Collection[KeyValue] = collection("backingStore", KeyValue)

  protected lazy val databaseInitialized: StoredValue[Boolean] = stored[Boolean]("_databaseInitialized", false)
  protected lazy val appliedUpgrades: StoredValue[Set[String]] = stored[Set[String]]("_appliedUpgrades", Set.empty)

  def initialized: Boolean = _initialized.get()

  protected[lightdb] def verifyInitialized(): Unit = if (!initialized) throw new RuntimeException("Database not initialized!")

  def init(truncate: Boolean = false): IO[Unit] = if (_initialized.compareAndSet(false, true)) {
    for {
      // Truncate the database before we do anything if specified
      _ <- this.truncate().whenA(truncate)
      // Determine if this is an uninitialized database
      dbInitialized <- databaseInitialized.get()
      // Get applied database upgrades
      applied <- appliedUpgrades.get()
      // Determine upgrades that need to be applied
      upgrades = this.upgrades.filter(u => u.alwaysRun || !applied.contains(u.label)) match {
        case list if !dbInitialized => list.filterNot(_.applyToNew)
        case list => list
      }
      _ <- (for {
        _ <- logger.info(s"Applying ${upgrades.length} upgrades (${upgrades.map(_.label).mkString(", ")})...")
        _ <- doUpgrades(upgrades, stillBlocking = true)
      } yield ()).whenA(upgrades.nonEmpty)
      // Set initialized
      _ <- databaseInitialized.set(true)
    } yield ()
  } else {
    IO.unit
  }

  def store[D <: Document[D]](collection: Collection[D]): ObjectStore
  def indexer[D <: Document[D]](collection: Collection[D]): Indexer[D]

  def upgrades: List[DatabaseUpgrade] = Nil

  def collection[D <: Document[D]](name: String, mapping: ObjectMapping[D]): Collection[D] = synchronized {
    val c = Collection[D](this, mapping, name)
    _collections = _collections ::: List(c)
    c
  }

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

  def truncate(): IO[Unit] = _collections.map(_.truncate()).parSequence.map(_ => ())

  def dispose(): IO[Unit] = _collections.map(_.dispose()).parSequence.map(_ => ())

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