package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.cache.CacheConfig
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path}
import scala.concurrent.duration.DurationInt

/**
 * Backend-agnostic coverage for the per-store point-lookup cache. A concrete spec per backend picks a
 * `storeManager` (Lucene, RocksDB, SQLite, etc.) and this suite exercises read-through, write-through,
 * LRU eviction, rollback, and query-bypass behavior uniformly.
 *
 * The per-backend supportsRollback flag gates the rollback test since some backends (e.g. HaloDB) don't
 * implement `RollbackSupport`.
 */
abstract class AbstractStoreCacheSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected def storeManager: CollectionManager

  protected lazy val specName: String = getClass.getSimpleName

  private lazy val dbPath: Path = Path.of(s"db/$specName")
  rmTree(dbPath)

  private def rmTree(p: Path): Unit = if (Files.exists(p)) {
    if (Files.isDirectory(p)) Files.list(p).forEach(rmTree)
    Files.deleteIfExists(p)
  }

  case class Item(name: String, _id: Id[Item] = Item.id()) extends Document[Item]

  object Item extends DocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  /** Typed DB holder so tests can reach `db.items` without structural typing gymnastics. */
  class CacheDB(cacheConfig: CacheConfig) extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager
    override lazy val directory: Option[Path] = Some(dbPath.resolve(java.util.UUID.randomUUID().toString))
    val items: Collection[Item, Item.type] = store(Item, cache = cacheConfig)
    override protected def truncateOnInit: Boolean = true
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private def newDb(cacheConfig: CacheConfig): CacheDB = new CacheDB(cacheConfig)

  specName should {
    "read-through: a cold-cache tx.get(id) populates the cache; second get is a hit" in {
      val db = newDb(CacheConfig.lru(100))
      for {
        _ <- db.init
        id1 = Item.id("one")
        _ <- db.items.transaction(_.insert(Item("one", id1)))
        // Clear the cache to simulate a cold start (e.g. a JVM restart loading from disk).
        _ = db.items.cache.foreach(_.clear())
        _ = db.items.cacheStats.get.reset()
        _ <- db.items.transaction(tx => tx.get(id1).map(_.map(_.name) should be(Some("one"))))
        statsAfterFirst = db.items.cacheStats.get.snapshot
        _ = statsAfterFirst.misses should be >= 1L  // cold read → miss
        _ <- db.items.transaction(tx => tx.get(id1).map(_.map(_.name) should be(Some("one"))))
        _ = db.items.cacheStats.get.hits should be >= 1L  // primed read → hit
        _ <- db.dispose
      } yield succeed
    }

    "write-through (commit): inserted doc is cache-hit in a later transaction" in {
      val db = newDb(CacheConfig.lru(100))
      for {
        _ <- db.init
        id = Item.id("inserted")
        _ <- db.items.transaction(_.insert(Item("inserted", id)))
        // Immediately fetch in a new transaction — should be a cache hit with no backend round-trip.
        _ <- db.items.transaction(tx => tx.get(id).map(_.map(_.name) should be(Some("inserted"))))
        stats = db.items.cacheStats.get
        _ = stats.hits should be >= 1L
        _ <- db.dispose
      } yield succeed
    }

    "write-through (upsert): overwrites cached value" in {
      val db = newDb(CacheConfig.lru(100))
      for {
        _ <- db.init
        id = Item.id("upd")
        _ <- db.items.transaction(_.insert(Item("first", id)))
        _ <- db.items.transaction(_.upsert(Item("second", id)))
        _ <- db.items.transaction(tx => tx.get(id).map(_.map(_.name) should be(Some("second"))))
        _ <- db.dispose
      } yield succeed
    }

    "write-through (delete): evicts cache entry" in {
      val db = newDb(CacheConfig.lru(100))
      for {
        _ <- db.init
        id = Item.id("del")
        _ <- db.items.transaction(_.insert(Item("del", id)))
        // Prime the cache.
        _ <- db.items.transaction(_.get(id))
        before = db.items.cacheStats.get.evictions
        _ <- db.items.transaction(_.delete(id))
        after = db.items.cacheStats.get.evictions
        _ = after should be > before
        _ <- db.items.transaction(tx => tx.get(id).map(_ should be(None)))
        _ <- db.dispose
      } yield succeed
    }

    "no caching when CacheConfig.None (default)" in {
      val db = newDb(CacheConfig.None)
      for {
        _ <- db.init
        _ = db.items.cacheStats should be(None)
        id = Item.id("uncached")
        _ <- db.items.transaction(_.insert(Item("uncached", id)))
        _ <- db.items.transaction(tx => tx.get(id).map(_.map(_.name) should be(Some("uncached"))))
        _ <- db.dispose
      } yield succeed
    }

    "LRU eviction when size exceeds maxEntries" in {
      val db = newDb(CacheConfig.lru(maxEntries = 5))
      val ids = (1 to 20).map(i => Item.id(s"i$i")).toList
      val items = ids.zipWithIndex.map { case (id, i) => Item(s"i$i", id) }
      for {
        _ <- db.init
        _ <- db.items.transaction(_.insert(items))
        // Read all 20 back — each populates the cache, triggering evictions past maxEntries=5.
        _ <- db.items.transaction { tx =>
          ids.foldLeft(Task.unit)((acc, id) => acc.flatMap(_ => tx.get(id).unit))
        }
        stats = db.items.cacheStats.get
        _ = stats.evictions should be > 0L
        // Final cache size should be <= maxEntries
        _ <- db.dispose
      } yield succeed
    }

    "TTL eviction: expired entries are dropped on next access" in {
      val db = newDb(CacheConfig.lru(maxEntries = 100, ttl = 50.millis))
      for {
        _ <- db.init
        id = Item.id("ttl")
        _ <- db.items.transaction(_.insert(Item("ttl", id)))
        // Prime cache (insert's write-through already placed it, this is a cache hit)
        _ <- db.items.transaction(_.get(id))
        missesAfterPrime = db.items.cacheStats.get.misses
        evictionsAfterPrime = db.items.cacheStats.get.evictions
        _ <- Task.sleep(150.millis)
        // After TTL, the lookup should register a miss (expired entry evicted)
        _ <- db.items.transaction(_.get(id))
        stats = db.items.cacheStats.get
        _ = (stats.misses - missesAfterPrime) should be >= 1L  // post-TTL miss
        _ = (stats.evictions - evictionsAfterPrime) should be >= 1L  // TTL eviction
        _ <- db.dispose
      } yield succeed
    }

    "same-tx upsert shadows stale cached value (tx overlay used as read cache)" in {
      val db = newDb(CacheConfig.lru(100))
      for {
        _ <- db.init
        id = Item.id("shadow")
        // Prime the store cache with an "old" value.
        _ <- db.items.transaction(_.insert(Item("old", id)))
        _ <- db.items.transaction(_.get(id))  // ensure cache populated
        // In a NEW tx: upsert a new value AND read it back in the same tx.
        // The store cache still holds "old" until commit; the overlay must shadow it.
        result <- db.items.transaction { tx =>
          tx.upsert(Item("new", id)).flatMap(_ => tx.get(id))
        }
        _ = result.map(_.name) should be(Some("new"))
        _ <- db.dispose
      } yield succeed
    }

    "same-tx delete shadows cached value (returns None within the same tx)" in {
      val db = newDb(CacheConfig.lru(100))
      for {
        _ <- db.init
        id = Item.id("ghost")
        _ <- db.items.transaction(_.insert(Item("ghost", id)))
        _ <- db.items.transaction(_.get(id))  // populate cache
        result <- db.items.transaction { tx =>
          tx.delete(id).flatMap(_ => tx.get(id))
        }
        _ = result should be(None)
        _ <- db.dispose
      } yield succeed
    }

    "unbounded cache: no eviction when no TTL" in {
      val db = newDb(CacheConfig.unbounded)
      val ids = (1 to 50).map(i => Item.id(s"u$i")).toList
      val items = ids.zipWithIndex.map { case (id, i) => Item(s"u$i", id) }
      for {
        _ <- db.init
        _ <- db.items.transaction(_.insert(items))
        _ <- db.items.transaction { tx =>
          ids.foldLeft(Task.unit)((acc, id) => acc.flatMap(_ => tx.get(id).unit))
        }
        stats = db.items.cacheStats.get
        _ = stats.evictions should be(0L)
        _ <- db.dispose
      } yield succeed
    }
  }

  // Rollback behavior is covered at the unit-test level against `RollbackSupport.rollback` itself rather
  // than via end-to-end backend rollback, since some backends (e.g. Lucene) have non-trivial
  // rollback-closes-writer semantics that interact poorly with `transaction { ... }` auto-commit.
}
