package lightdb.store

import lightdb.LightDB
import lightdb.cache.CacheConfig
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.multi.MultiStore
import lightdb.transaction.Transaction

/**
 * Fluent, immutable builder for `Store` and `MultiStore` construction. Returned by
 * [[LightDB.store]]. Materialize via one of the terminators:
 *
 *   - `apply()` — returns a single `Store`
 *   - `multi(keys, namePrefix?)` — returns a `MultiStore` keyed by `Key`
 *
 * Examples:
 * {{{
 *   val items   = db.store(Item)()
 *   val cached  = db.store(Item).withCache(CacheConfig.lru(1000))()
 *   val split   = db.store(Item).withMode(StoreMode.Indexes(storage))()
 *   val custom  = db.store(Item).withStoreManager(SQLiteStore).withCache(...)()
 *   val shards  = db.store(Item).withCache(...).multi(List("a", "b", "c"))
 * }}}
 *
 * The builder is generic in the [[StoreManager]] type so chained `.withStoreManager(...)` returns a
 * re-parameterized builder whose terminal `apply()` produces the correct `sm.S[Doc, Model]`. Each
 * `withX` method returns a NEW builder (no mutation).
 */
final class StoreBuilder[Doc <: Document[Doc], Model <: DocumentModel[Doc], SM <: StoreManager] private[lightdb] (
  private val db: LightDB,
  private val model: Model,
  val sm: SM,
  private val name: Option[String] = None,
  private val mode: Option[StoreMode[Doc, Model]] = None,
  private val cache: CacheConfig = CacheConfig.None
) {
  /** Override the store name. Defaults to `model.modelName`. Used as the full name for `apply()`. */
  def withName(name: String): StoreBuilder[Doc, Model, SM] =
    new StoreBuilder(db, model, sm, Some(name), mode, cache)

  /** Override the [[StoreMode]]. Defaults to `StoreMode.All()`. */
  def withMode(mode: StoreMode[Doc, Model]): StoreBuilder[Doc, Model, SM] =
    new StoreBuilder(db, model, sm, name, Some(mode), cache)

  /** Configure the per-store point-lookup cache. Defaults to `CacheConfig.None`. */
  def withCache(cache: CacheConfig): StoreBuilder[Doc, Model, SM] =
    new StoreBuilder(db, model, sm, name, mode, cache)

  /** Use a custom [[StoreManager]] instead of the database's default. Re-parameterizes the builder. */
  def withStoreManager[SM2 <: StoreManager](sm2: SM2): StoreBuilder[Doc, Model, SM2] =
    new StoreBuilder[Doc, Model, SM2](db, model, sm2, name, mode, cache)

  /**
   * Materialize a single store, register it with the database, and return it. Path-dependent on the
   * configured `sm`.
   */
  def apply(): sm.S[Doc, Model] = {
    val n = name.getOrElse(model.modelName)
    val path = db.directory.map(_.resolve(n))
    val effectiveMode = mode.getOrElse(StoreMode.All[Doc, Model]())
    val store = sm.create[Doc, Model](db, model, n, path, effectiveMode)
    store.configureCache(cache)
    db.registerStore(store)
    store
  }

  /**
   * Materialize a [[MultiStore]] — a logical grouping of `keys.size` underlying stores keyed by
   * `Key`. Each underlying store is built with this builder's configuration (mode, cache,
   * storeManager). Names are derived as `s"${namePrefix.getOrElse(model.modelName)}_${key2Name(key)}"`.
   *
   * `withName` is intentionally NOT used here — supply `namePrefix` to override the default.
   */
  def multi[Key](
    keys: Iterable[Key],
    namePrefix: Option[String] = None
  )(implicit key2Name: Key => String):
      MultiStore[Doc, Model, sm.S[Doc, Model], Key] = {
    val prefix = namePrefix.getOrElse(model.modelName)
    val effectiveMode = mode.getOrElse(StoreMode.All[Doc, Model]())
    val storesMap: Map[Key, sm.S[Doc, Model]] = keys.toList.map { key =>
      val storeName = s"${prefix}_${key2Name(key)}"
      val path = db.directory.map(_.resolve(storeName))
      val s = sm.create[Doc, Model](db, model, storeName, path, effectiveMode)
      s.configureCache(cache)
      db.registerStore(s)
      key -> s
    }.toMap
    new MultiStore[Doc, Model, sm.S[Doc, Model], Key](storesMap)
  }
}
