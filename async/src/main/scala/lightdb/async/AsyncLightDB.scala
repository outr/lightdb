package lightdb.async

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fabric.rw.RW
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel}
import lightdb.feature.{DBFeatureKey, FeatureSupport}
import lightdb.{KeyValue, LightDB, Persistence, StoredValue}
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade

import java.nio.file.Path

trait AsyncLightDB extends FeatureSupport[DBFeatureKey] { db =>
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

  override def put[T](key: DBFeatureKey[T], value: T): Unit = underlying.put(key, value)

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
                                                                    storeManager: Option[StoreManager] = None,
                                                                    maxInsertBatch: Int = 1_000_000,
                                                                    cacheQueries: Boolean = Collection.DefaultCacheQueries): AsyncCollection[Doc, Model] =
    AsyncCollection(underlying.collection[Doc, Model](model, name, store, storeManager, maxInsertBatch, cacheQueries))

  object stored {
    def apply[T](key: String,
                 default: => T,
                 persistence: Persistence = Persistence.Stored,
                 collection: AsyncCollection[KeyValue, KeyValue.type] = backingStore)
                (implicit rw: RW[T]): AsyncStoredValue[T] = AsyncStoredValue(underlying.stored[T](
      key = key,
      default = default,
      persistence = persistence,
      collection = collection.underlying
    ))

    def opt[T](key: String,
               persistence: Persistence = Persistence.Stored,
               collection: AsyncCollection[KeyValue, KeyValue.type] = backingStore)
              (implicit rw: RW[T]): AsyncStoredValue[Option[T]] = AsyncStoredValue(underlying.stored.opt[T](
      key = key,
      persistence = persistence,
      collection = collection.underlying
    ))
  }

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
