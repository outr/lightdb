package lightdb.cache

import lightdb.id.Id

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

/**
 * Per-store, in-memory point-lookup cache. Hand-rolled (no third-party deps) and backed by
 * `ConcurrentHashMap` to match the convention set by [[QueryCache]].
 *
 *   - Reads ([[get]]) are O(1) for hits; misses do not insert (caller is responsible for `put` after a
 *     backend lookup succeeds).
 *   - TTL (when configured) is checked lazily on `get` — expired entries are removed and treated as a miss.
 *   - LRU eviction (in [[LruStoreCache]]) fires inside `put` when the entry count exceeds the configured
 *     `maxEntries`; entries are removed in oldest-access-first order.
 *
 * Concurrent populate of the same id from multiple threads is allowed — the last `put` wins. This avoids
 * any locking on the read path and is correct because all values for the same id are equivalent (same
 * underlying doc state). Strict single-load semantics (e.g. via `computeIfAbsent`) can be added later if
 * we observe thundering-herd loads under contention.
 */
trait StoreCache[Doc] {
  /** Lookup an id. Returns `None` if absent or expired. Records hit/miss in [[stats]]. */
  def get(id: Id[Doc]): Option[Doc]

  /** Insert or replace the cached value for `id`. */
  def put(id: Id[Doc], doc: Doc): Unit

  /** Remove the cached entry for `id`, if any. Counts as an eviction in [[stats]]. */
  def evict(id: Id[Doc]): Unit

  /** Drop all entries. */
  def clear(): Unit

  /** Number of entries currently in the cache (approximate under concurrent mutation). */
  def size: Int

  /** Hit/miss/eviction counters. */
  def stats: CacheStats
}

object StoreCache {
  /** Build a cache from a [[CacheConfig]]. Returns `None` for [[CacheConfig.None]]. */
  def fromConfig[Doc](config: CacheConfig): Option[StoreCache[Doc]] = config match {
    case CacheConfig.None => scala.None
    case CacheConfig.Lru(maxEntries, ttl) => Some(new LruStoreCache[Doc](maxEntries, ttl))
    case CacheConfig.Unbounded(ttl) => Some(new UnboundedStoreCache[Doc](ttl))
  }

  // --------------------------------------------------------------------------------------------------
  // Internal entry record. `lastAccess` is volatile so reads see fresh values without per-entry locking.
  // --------------------------------------------------------------------------------------------------
  private[cache] final class Entry[Doc](val doc: Doc, @volatile var lastAccess: Long) {
    def touch(now: Long): Unit = lastAccess = now
    def isExpired(now: Long, ttlNanos: Long): Boolean =
      ttlNanos > 0L && (now - lastAccess) > ttlNanos
  }

  private[cache] def now(): Long = System.nanoTime()

  private[cache] def ttlNanos(ttl: Option[FiniteDuration]): Long =
    ttl.fold(0L)(_.toNanos)
}

// ----------------------------------------------------------------------------------------------------
// Unbounded — no size cap. TTL (if configured) is checked lazily on access.
// ----------------------------------------------------------------------------------------------------
final class UnboundedStoreCache[Doc](ttl: Option[FiniteDuration]) extends StoreCache[Doc] {
  import StoreCache.{Entry, now, ttlNanos}

  private val ttlNs = ttlNanos(ttl)
  private val map = new ConcurrentHashMap[Id[Doc], Entry[Doc]]()
  override val stats: CacheStats = new CacheStats

  override def get(id: Id[Doc]): Option[Doc] = {
    val entry = map.get(id)
    if (entry == null) {
      stats.recordMiss()
      None
    } else {
      val n = now()
      if (entry.isExpired(n, ttlNs)) {
        if (map.remove(id, entry)) stats.recordEviction()
        stats.recordMiss()
        None
      } else {
        entry.touch(n)
        stats.recordHit()
        Some(entry.doc)
      }
    }
  }

  override def put(id: Id[Doc], doc: Doc): Unit = {
    map.put(id, new Entry[Doc](doc, now()))
  }

  override def evict(id: Id[Doc]): Unit = {
    if (map.remove(id) != null) stats.recordEviction()
  }

  override def clear(): Unit = {
    val n = map.size()
    map.clear()
    if (n > 0) stats.recordEviction(n.toLong)
  }

  override def size: Int = map.size()
}

// ----------------------------------------------------------------------------------------------------
// LRU — bounded by `maxEntries`. On insert, if size > maxEntries, evict oldest-access-first entries
// until back under the cap. TTL (if configured) is checked lazily on access.
// ----------------------------------------------------------------------------------------------------
final class LruStoreCache[Doc](maxEntries: Int, ttl: Option[FiniteDuration]) extends StoreCache[Doc] {
  import StoreCache.{Entry, now, ttlNanos}

  require(maxEntries > 0, s"maxEntries must be > 0, got $maxEntries")

  private val ttlNs = ttlNanos(ttl)
  private val map = new ConcurrentHashMap[Id[Doc], Entry[Doc]]()
  override val stats: CacheStats = new CacheStats

  override def get(id: Id[Doc]): Option[Doc] = {
    val entry = map.get(id)
    if (entry == null) {
      stats.recordMiss()
      None
    } else {
      val n = now()
      if (entry.isExpired(n, ttlNs)) {
        if (map.remove(id, entry)) stats.recordEviction()
        stats.recordMiss()
        None
      } else {
        entry.touch(n)
        stats.recordHit()
        Some(entry.doc)
      }
    }
  }

  override def put(id: Id[Doc], doc: Doc): Unit = {
    map.put(id, new Entry[Doc](doc, now()))
    // Cheap fast-path: most puts won't trigger an eviction sweep.
    if (map.size() > maxEntries) evictOldest()
  }

  override def evict(id: Id[Doc]): Unit = {
    if (map.remove(id) != null) stats.recordEviction()
  }

  override def clear(): Unit = {
    val n = map.size()
    map.clear()
    if (n > 0) stats.recordEviction(n.toLong)
  }

  override def size: Int = map.size()

  // Evict the oldest-access entries until size is back at or below maxEntries. Sweep is performed under
  // a synchronized block so concurrent puts don't double-evict (the eviction sort is the cost we pay
  // for not maintaining a separate access-order linked list).
  private def evictOldest(): Unit = synchronized {
    val excess = map.size() - maxEntries
    if (excess <= 0) return
    val candidates = map.entrySet().asScala.toArray
    java.util.Arrays.sort(
      candidates.asInstanceOf[Array[java.util.Map.Entry[Id[Doc], Entry[Doc]]]],
      (a: java.util.Map.Entry[Id[Doc], Entry[Doc]], b: java.util.Map.Entry[Id[Doc], Entry[Doc]]) =>
        java.lang.Long.compare(a.getValue.lastAccess, b.getValue.lastAccess)
    )
    var removed = 0
    var i = 0
    while (i < candidates.length && removed < excess) {
      val e = candidates(i)
      if (map.remove(e.getKey, e.getValue)) removed += 1
      i += 1
    }
    if (removed > 0) stats.recordEviction(removed.toLong)
  }
}
