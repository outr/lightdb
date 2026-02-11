package lightdb.traversal.store

import lightdb.{KeyValue, LightDB}
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.{StoreManager, StoreMode}
import lightdb.store.prefix.{PrefixScanningStore, PrefixScanningStoreManager}
import lightdb.store.Collection
import lightdb.transaction.batch.BatchConfig
import profig.Profig
import fabric.*
import fabric.rw.*
import rapid.Task

import java.nio.file.Path

/**
 * Reference traversal-backed store.
 *
 * This store is correctness-first and wraps a provided PrefixScanningStore for persistence
 * (SplitStore-style composition).
 * Later iterations will add index keyspaces and optimized execution.
 */
class TraversalStore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](name: String,
                                                                       path: Option[Path],
                                                                       model: Model,
                                                                       val backing: PrefixScanningStore[Doc, Model],
                                                                       // Optional dedicated persisted-index store for this collection.
                                                                       // When provided, persisted index writes/reads will use this store instead of `_backingStore`.
                                                                       val indexBacking: Option[PrefixScanningStore[KeyValue, KeyValue.type]] = None,
                                                                       lightDB: LightDB,
                                                                       storeManager: StoreManager)
  extends Collection[Doc, Model](name, path, model, lightDB, storeManager)
    with PrefixScanningStore[Doc, Model] {
  override type TX = TraversalTransaction[Doc, Model]

  // Traversal currently does not expose NestedQueryStore capability.
  // Nested filters are rejected at Query validation time for this backend.
  override def supportsNestedQueries: Boolean = false

  /**
   * Persisted index writes (postings in `effectiveIndexBacking`) are enabled by default for scaling.
   *
   * Disable explicitly with:
   * -Dlightdb.traversal.persistedIndex.enabled=false
   */
  private[traversal] val persistedIndexEnabled: Boolean =
    Profig("lightdb.traversal.persistedIndex.enabled").opt[Boolean].getOrElse(true)

  /**
   * When enabled, non-empty collections will automatically build/backfill the persisted postings index on init if the
   * index is not ready yet.
   *
   * This is intentionally opt-in because it can be expensive for very large datasets.
   *
   * Enable with:
   * -Dlightdb.traversal.persistedIndex.autobuild=true
   */
  private val persistedIndexAutoBuild: Boolean =
    Profig("lightdb.traversal.persistedIndex.autobuild").opt[Boolean].getOrElse(false)

  lazy val effectiveIndexBacking: Option[PrefixScanningStore[KeyValue, KeyValue.type]] = {
    indexBacking.orElse {
      // Auto-create a dedicated on-disk index store when persisted indexing is enabled and the backing store
      // uses a PrefixScanningStoreManager (e.g. RocksDBStore).
      if !persistedIndexEnabled then None
      else {
        backing.storeManager match {
          case psm: PrefixScanningStoreManager =>
            val idxName = s"${name}__tindex"
            val idxPath = path.map(p => p.getParent.resolve(idxName))
            Some(
              psm
                .create[KeyValue, KeyValue.type](
                  lightDB,
                  KeyValue,
                  idxName,
                  idxPath,
                  StoreMode.All[KeyValue, KeyValue.type]()
                )
                .asInstanceOf[PrefixScanningStore[KeyValue, KeyValue.type]]
            )
          case _ =>
            None
        }
      }
    }
  }

  private[traversal] val indexCache: TraversalIndexCache[Doc, Model] =
    new TraversalIndexCache[Doc, Model](
      storeName = name,
      model = model,
      enabled = Profig("lightdb.traversal.indexCache").opt[Boolean].getOrElse(false)
    )

  /**
   * Opt-in: allow traversal to handle ExistsChild without planner resolution.
   *
   * Default is false. When enabled, traversal will:
   * - use an early-terminating semi-join for "page-only" queries where possible, and
   * - otherwise fall back to resolving ExistsChild via FilterPlanner (correctness-first).
   *
   * Enable with:
   * -Dlightdb.traversal.existsChild.native=true
   */
  override def supportsNativeExistsChild: Boolean =
    {
      def boolProfig(key: String, default: Boolean): Boolean = {
        Profig(key).get() match {
          case Some(Bool(b, _)) => b
          case Some(o: Obj) =>
            // Allow object-style flags, e.g. `lightdb.traversal.existsChild.nativeFull.maxParentIds=1000`
            // In that case `nativeFull` becomes an object; treat it as enabled unless explicitly disabled.
            o.get("enabled") match {
              case Some(Bool(b, _)) => b
              case Some(other) => other.asBoolean
              case None => true
            }
          case _ =>
            default
        }
      }

      boolProfig("lightdb.traversal.existsChild.native", default = false) ||
        boolProfig("lightdb.traversal.existsChild.nativeFull", default = false)
    }

  override def storeMode: StoreMode[Doc, Model] = backing.storeMode

  override protected def initialize(): Task[Unit] =
    backing.init.next {
      effectiveIndexBacking.map(_.init).getOrElse(Task.unit)
    }.next {
      // Enforce invariant: if persisted indexing is enabled for a real collection, an index backing store must exist.
      // Otherwise we'd silently fall back to scanning (bad for scale and surprising for users).
      if persistedIndexEnabled && name != "_backingStore" && effectiveIndexBacking.isEmpty then {
        Task {
          throw new IllegalStateException(
            s"TraversalStore('$name') has persisted indexing enabled, but no index backing store is available. " +
              s"Either pass `indexBacking` to TraversalStore, or set -Dlightdb.traversal.persistedIndex=false. " +
              s"(Auto-create requires a PrefixScanningStoreManager backing and a usable path.)"
          )
        }
      } else Task.unit
    }.next {
      // Persisted index seeding must only be used when the index is known complete.
      // For a brand-new empty collection, we can safely mark the persisted index "ready" up-front.
      // Existing collections must call a backfill/build to safely enable ready-based candidate seeding.
      if persistedIndexEnabled && name != "_backingStore" then {
        effectiveIndexBacking match {
          case Some(idx) =>
            // Determine whether the persisted index is already ready.
            val readyT: Task[Boolean] = idx.transaction.withStoreNativeBatch { kv =>
              TraversalPersistedIndex.isReady(name, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
            }.attempt.map(_.getOrElse(false))

            // Determine whether the primary store is empty.
            val emptyT: Task[Boolean] = backing.transaction(_.count).map(_ == 0)

            (for
              ready <- readyT
              empty <- emptyT
              _ <-
                if ready then Task.unit
                else if empty then {
                  idx.transaction.withStoreNativeBatch { kv =>
                    TraversalPersistedIndex.markReady(name, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
                  }.attempt.unit
                } else if persistedIndexAutoBuild then {
                  buildPersistedIndex()
                } else Task.unit
            yield ()).attempt.unit
          case None => Task.unit
        }
      } else Task.unit
    }.next(super.initialize())

  override protected def createTransaction(parent: Option[lightdb.transaction.Transaction[Doc, Model]],
                                           batchConfig: BatchConfig,
                                           writeHandlerFactory: lightdb.transaction.Transaction[Doc, Model] => lightdb.transaction.WriteHandler[Doc, Model]): Task[TX] = for
    t <- Task(TraversalTransaction(this, parent, writeHandlerFactory))
    bt <- backing.transaction.withParent(t).withBatch(batchConfig).create()
    _ = t._backing = bt.asInstanceOf[t.store.backing.TX]
  yield t

  override protected def doDispose(): Task[Unit] =
    effectiveIndexBacking.map(_.dispose).getOrElse(Task.unit).next(backing.dispose).next(super.doDispose())

  /**
   * Explicitly (re)builds the persisted postings index for this collection and marks it ready for candidate seeding.
   *
   * This is intended for:
   * - migrating an existing collection to traversal persisted indexing
   * - recovering after index corruption / deletion
   */
  def buildPersistedIndex(): Task[Unit] =
    if !persistedIndexEnabled || name == "_backingStore" then Task.unit
    else {
      (effectiveIndexBacking match {
        case Some(idx) =>
          backing.transaction { bt =>
            bt.flush.next {
              idx.transaction.withStoreNativeBatch { kv =>
                TraversalPersistedIndex.buildFromStore(
                  storeName = name,
                  model = model,
                  backing = bt,
                  kv = kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]]
                )
              }
            }
          }
        case None =>
          Task.unit
      }).unit
    }

  /**
   * True if the persisted postings index is safe to use for candidate seeding.
   */
  def persistedIndexReady(): Task[Boolean] =
    if !persistedIndexEnabled || name == "_backingStore" then Task.pure(false)
    else effectiveIndexBacking match {
      case Some(idx) =>
        idx.transaction.withStoreNativeBatch { kv =>
          TraversalPersistedIndex.isReady(name, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
        }.attempt.map(_.getOrElse(false))
      case None =>
        Task.pure(false)
    }

  private[traversal] def markPersistedIndexNotReady(): Task[Unit] =
    if !persistedIndexEnabled || name == "_backingStore" then Task.unit
    else effectiveIndexBacking match {
      case Some(idx) =>
        idx.transaction.withStoreNativeBatch { kv =>
          TraversalPersistedIndex.markNotReady(name, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
        }.attempt.unit
      case None =>
        Task.unit
    }

  /**
   * Exposes persisted equality postings for diagnostics/tests.
   */
  def persistedEqPostings(fieldName: String, value: Any): Task[Set[String]] =
    if !persistedIndexEnabled || name == "_backingStore" then Task.pure(Set.empty)
    else effectiveIndexBacking match {
      case Some(idx) =>
        idx.transaction.withStoreNativeBatch { kv =>
          TraversalPersistedIndex.eqPostings(name, fieldName, value, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
        }.attempt.map(_.getOrElse(Set.empty))
      case None =>
        Task.pure(Set.empty)
    }

  /**
   * Exposes persisted n-gram postings for diagnostics/tests.
   */
  def persistedNgPostings(fieldName: String, query: String): Task[Set[String]] =
    if !persistedIndexEnabled || name == "_backingStore" then Task.pure(Set.empty)
    else effectiveIndexBacking match {
      case Some(idx) =>
        idx.transaction.withStoreNativeBatch { kv =>
          TraversalPersistedIndex.ngPostings(name, fieldName, query, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
        }.attempt.map(_.getOrElse(Set.empty))
      case None =>
        Task.pure(Set.empty)
    }

  /**
   * Exposes persisted startsWith postings for diagnostics/tests.
   */
  def persistedSwPostings(fieldName: String, query: String): Task[Set[String]] =
    if !persistedIndexEnabled || name == "_backingStore" then Task.pure(Set.empty)
    else effectiveIndexBacking match {
      case Some(idx) =>
        idx.transaction.withStoreNativeBatch { kv =>
          TraversalPersistedIndex.swPostings(name, fieldName, query, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
        }.attempt.map(_.getOrElse(Set.empty))
      case None =>
        Task.pure(Set.empty)
    }

  /**
   * Exposes persisted endsWith postings for diagnostics/tests.
   */
  def persistedEwPostings(fieldName: String, query: String): Task[Set[String]] =
    if !persistedIndexEnabled || name == "_backingStore" then Task.pure(Set.empty)
    else effectiveIndexBacking match {
      case Some(idx) =>
        idx.transaction.withStoreNativeBatch { kv =>
          TraversalPersistedIndex.ewPostings(name, fieldName, query, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
        }.attempt.map(_.getOrElse(Set.empty))
      case None =>
        Task.pure(Set.empty)
    }

  /**
   * Exposes persisted range-long postings for diagnostics/tests.
   */
  def persistedRangeLongPostings(fieldName: String, from: Option[Long], to: Option[Long]): Task[Set[String]] =
    if !persistedIndexEnabled || name == "_backingStore" then Task.pure(Set.empty)
    else effectiveIndexBacking match {
      case Some(idx) =>
        idx.transaction.withStoreNativeBatch { kv =>
          TraversalPersistedIndex.rangeLongPostings(name, fieldName, from, to, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
        }.attempt.map(_.getOrElse(Set.empty))
      case None =>
        Task.pure(Set.empty)
    }

  /**
   * Exposes persisted range-double postings for diagnostics/tests.
   */
  def persistedRangeDoublePostings(fieldName: String, from: Option[Double], to: Option[Double]): Task[Set[String]] =
    if !persistedIndexEnabled || name == "_backingStore" then Task.pure(Set.empty)
    else effectiveIndexBacking match {
      case Some(idx) =>
        idx.transaction.withStoreNativeBatch { kv =>
          TraversalPersistedIndex.rangeDoublePostings(name, fieldName, from, to, kv.asInstanceOf[lightdb.transaction.PrefixScanningTransaction[KeyValue, KeyValue.type]])
        }.attempt.map(_.getOrElse(Set.empty))
      case None =>
        Task.pure(Set.empty)
    }

  // No special verification beyond base Store.initialize; indexes will be added later.
}


