package benchmark.jmh.complete

import lightdb.LightDB
import lightdb.chroniclemap.ChronicleMapStore
import lightdb.duckdb.DuckDBStore
import lightdb.h2.H2Store
import lightdb.halodb.HaloDBStore
import lightdb.lmdb.LMDBStore
import lightdb.lucene.LuceneStore
import lightdb.mapdb.MapDBStore
import lightdb.rocksdb.RocksDBStore
import lightdb.sql.SQLiteStore
import lightdb.store.hashmap.HashMapStore
import lightdb.store.split.SplitStoreManager
import lightdb.store.{CollectionManager, StoreManager}
import lightdb.tantivy.TantivyStore
import lightdb.upgrade.DatabaseUpgrade

import java.nio.file.Path

/** Single registry of every backend the complete benchmark exercises.
 *
 *  Three categories — kept distinct because not every workload applies to every category:
 *  - **Kv**: only `_id`-keyed ops are well defined (`get/upsert/delete/scan`).
 *  - **Collection**: adds the `Query` DSL (filters, sort, count, aggregations).
 *  - **Split**: a `SplitStoreManager(storage, search)` pair — durable doc storage in `storage`,
 *    indexes in `search`. Reads come from `storage`, queries from `search`.
 *
 *  External-service backends (OpenSearch, PostgreSQL, Redis, MongoDB, Google Sheets) are
 *  intentionally excluded so the suite runs end-to-end with no setup.
 */
object BackendCatalog {
  enum Category {
    case Kv, Collection, Split
  }

  /** A backend descriptor. `build(dir)` returns a fresh `LightDB` exposing the typed `kv` /
   *  `coll` store named "bench". Caller is responsible for `init`/`dispose`.
   */
  final case class Spec(
    name: String,
    category: Category,
    isCollection: Boolean,
    build: Option[Path] => BenchDb
  )

  // ---- Kv-only backends -------------------------------------------------------------------

  val hashmap     : Spec = kv("hashmap",     onlyMemory = true,  HashMapStore)
  val mapdb       : Spec = kv("mapdb",       onlyMemory = false, MapDBStore)
  val lmdb        : Spec = kv("lmdb",        onlyMemory = false, LMDBStore)
  val rocksdb     : Spec = kv("rocksdb",     onlyMemory = false, RocksDBStore)
  val halodb      : Spec = kv("halodb",      onlyMemory = false, HaloDBStore)
  val chronicle   : Spec = kv("chroniclemap", onlyMemory = false, ChronicleMapStore)

  // ---- Collection backends ---------------------------------------------------------------

  val sqlite : Spec = collection("sqlite",  onlyMemory = false, SQLiteStore)
  val h2     : Spec = collection("h2",      onlyMemory = false, H2Store)
  val duckdb : Spec = collection("duckdb",  onlyMemory = false, DuckDBStore)
  val lucene : Spec = collection("lucene",  onlyMemory = false, LuceneStore)
  val tantivy: Spec = collection("tantivy", onlyMemory = false, TantivyStore)

  // ---- Split combinations (durable storage × search index) -------------------------------

  val rocksdbLucene  : Spec = split("rocksdb+lucene",  RocksDBStore,  LuceneStore)
  val rocksdbTantivy : Spec = split("rocksdb+tantivy", RocksDBStore,  TantivyStore)
  val lmdbLucene     : Spec = split("lmdb+lucene",     LMDBStore,     LuceneStore)
  val lmdbTantivy    : Spec = split("lmdb+tantivy",    LMDBStore,     TantivyStore)
  val halodbLucene   : Spec = split("halodb+lucene",   HaloDBStore,   LuceneStore)
  val halodbTantivy  : Spec = split("halodb+tantivy",  HaloDBStore,   TantivyStore)

  /** Every backend the benchmarks know about. Used as the master `@Param` list for whole-suite
   *  benchmarks; subset suites filter by `category` / `isCollection`.
   */
  val all: List[Spec] = List(
    hashmap, mapdb, lmdb, rocksdb, halodb, chronicle,
    sqlite, h2, duckdb, lucene, tantivy,
    rocksdbLucene, rocksdbTantivy, lmdbLucene, lmdbTantivy, halodbLucene, halodbTantivy
  )

  val allNames: Array[String] = all.map(_.name).toArray

  /** Names of every backend that supports query operations (i.e. Collection or Split). */
  val collectionNames: Array[String] =
    all.filter(_.isCollection).map(_.name).toArray

  /** Names of every Kv-only (non-collection) backend. */
  val kvOnlyNames: Array[String] =
    all.filterNot(_.isCollection).map(_.name).toArray

  def find(name: String): Spec =
    all.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown backend: $name"))

  // ---- Internals --------------------------------------------------------------------------

  private def kv[SM <: StoreManager](name: String, onlyMemory: Boolean, sm: SM): Spec =
    Spec(name, Category.Kv, isCollection = false, dir => BenchDb.kv(sm, if onlyMemory then None else dir))

  private def collection[SM <: CollectionManager](name: String, onlyMemory: Boolean, sm: SM): Spec =
    Spec(name, Category.Collection, isCollection = true, dir => BenchDb.collection(sm, if onlyMemory then None else dir))

  private def split[Storage <: StoreManager, Search <: CollectionManager](
    name: String, storage: Storage, search: Search
  ): Spec = {
    val sm = SplitStoreManager(storage, search)
    Spec(name, Category.Split, isCollection = true, dir => BenchDb.collection(sm, dir))
  }
}
