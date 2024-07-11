package lightdb.async

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.{KeyValue, LightDB}
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade

import java.nio.file.Path

trait AsyncLightDB { db =>
  object underlying extends LightDB {
    override def name: String = db.name
    override def directory: Option[Path] = db.directory
    override def storeManager: StoreManager = db.storeManager

    override protected def truncateOnInit: Boolean = db.truncateOnInit

    override protected def initialize(): Unit = {
      super.initialize()

      db.initialize().unsafeRunSync()
    }

    override def dispose(): Unit = {
      super.dispose()

      db.initialize().unsafeRunSync()
    }

    override lazy val upgrades: List[DatabaseUpgrade] = db.upgrades.map { u =>
      new DatabaseUpgrade {
        override def label: String = u.label
        override def applyToNew: Boolean = u.applyToNew
        override def blockStartup: Boolean = u.blockStartup
        override def alwaysRun: Boolean = u.alwaysRun

        override def upgrade(ldb: LightDB): Unit = u.upgrade(db).unsafeRunSync()
      }
    }
  }

  def backingStore: AsyncCollection[KeyValue, KeyValue.type] = AsyncCollection(underlying.backingStore)

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
  def upgrades: List[AsyncDatabaseUpgrade]

  /**
   * Automatically truncates all collections in the database during initialization if this is set to true.
   * Defaults to false.
   */
  protected def truncateOnInit: Boolean = false

  def collection[Doc <: Document[Doc], Model <: DocumentModel[Doc]](model: Model,
                                                   name: Option[String] = None,
                                                   store: Option[Store[Doc, Model]] = None,
                                                   maxInsertBatch: Int = 1_000_000,
                                                   cacheQueries: Boolean = Collection.DefaultCacheQueries): AsyncCollection[Doc, Model] =
    AsyncCollection(underlying.collection[Doc, Model](model, name, store, maxInsertBatch, cacheQueries))

  // TODO: AsyncStored

  final def init(): IO[Boolean] = IO.blocking(underlying.init()).flatMap {
    case true => initialize().map(_ => true)
    case false => IO.pure(false)
  }

  protected def initialize(): IO[Unit] = IO.unit

  def dispose(): IO[Boolean] = IO.blocking {
    if (underlying.disposed) {
      false
    } else {
      underlying.dispose()
      true
    }
  }
}
