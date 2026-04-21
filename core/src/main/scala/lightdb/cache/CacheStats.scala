package lightdb.cache

import java.util.concurrent.atomic.LongAdder

/**
 * Thread-safe hit/miss/eviction counters for a [[StoreCache]]. Backed by `LongAdder` for low-contention
 * concurrent updates.
 */
final class CacheStats {
  private val hitsAdder = new LongAdder
  private val missesAdder = new LongAdder
  private val evictionsAdder = new LongAdder

  def hits: Long = hitsAdder.sum()
  def misses: Long = missesAdder.sum()
  def evictions: Long = evictionsAdder.sum()

  /** Cache hit rate as a fraction in [0.0, 1.0]. Returns 0.0 when there have been no lookups. */
  def hitRate: Double = {
    val h = hits
    val m = misses
    val total = h + m
    if (total == 0L) 0.0 else h.toDouble / total.toDouble
  }

  /** Reset all counters to zero. */
  def reset(): Unit = {
    hitsAdder.reset()
    missesAdder.reset()
    evictionsAdder.reset()
  }

  /** Snapshot the current values atomically-enough for reporting (each adder summed independently). */
  def snapshot: CacheStats.Snapshot = CacheStats.Snapshot(hits, misses, evictions)

  // -- Internal increment hooks (called by StoreCache impls) ----------------------------------------------

  private[cache] def recordHit(): Unit = hitsAdder.increment()
  private[cache] def recordMiss(): Unit = missesAdder.increment()
  private[cache] def recordEviction(n: Long = 1L): Unit = evictionsAdder.add(n)
}

object CacheStats {
  final case class Snapshot(hits: Long, misses: Long, evictions: Long) {
    def total: Long = hits + misses
    def hitRate: Double = if (total == 0L) 0.0 else hits.toDouble / total.toDouble
  }
}
