package lightdb.cache

import scala.concurrent.duration.FiniteDuration

/**
 * Per-store, point-lookup cache configuration. Used by `LightDB.store(model, name, cache = ...)` to
 * opt a store into in-memory caching of `Transaction.get(id)` results.
 *
 * Query scans (`tx.query.filter(...).toList`, etc.) intentionally bypass the cache.
 *
 * Single-JVM only — no distributed coherence.
 */
sealed trait CacheConfig

object CacheConfig {
  /** No caching. The default. */
  case object None extends CacheConfig

  /** Bounded LRU/LFU cache. `ttl = None` means no time-based expiry — entries only evicted on capacity. */
  final case class Lru(maxEntries: Int, ttl: Option[FiniteDuration] = scala.None) extends CacheConfig

  /** Unbounded cache. Use for stores with a small fixed-size working set. `ttl = None` means never expires. */
  final case class Unbounded(ttl: Option[FiniteDuration] = scala.None) extends CacheConfig

  // -- Convenience builders -------------------------------------------------------------------------------

  val none: CacheConfig = None

  def lru(maxEntries: Int, ttl: FiniteDuration): CacheConfig = Lru(maxEntries, Some(ttl))
  def lru(maxEntries: Int): CacheConfig = Lru(maxEntries, scala.None)

  def unbounded(ttl: FiniteDuration): CacheConfig = Unbounded(Some(ttl))
  val unbounded: CacheConfig = Unbounded(scala.None)
}
