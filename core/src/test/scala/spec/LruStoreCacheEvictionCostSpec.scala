package spec

import lightdb.cache.{CacheStats, LruStoreCache}
import lightdb.id.Id
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.lang.management.ManagementFactory

/**
 * An LRU store cache at its cap evicts one entry per insert at a cost that does not grow with the cache.
 * The cost is measured as the bytes the inserting thread allocates per insert (a per-thread counter, so
 * other load on the machine does not move it): an eviction that copies or sorts the whole cache allocates
 * in proportion to its size.
 */
class LruStoreCacheEvictionCostSpec extends AnyWordSpec with Matchers {
  private val threads = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]

  private def bytesPerEvictingPut(cap: Int): Long = {
    val puts = 2000
    val cache = new LruStoreCache[String](cap, None)
    val ids = Array.tabulate(cap + puts * 2)(i => Id[String](s"id-$i"))
    (0 until cap).foreach(i => cache.put(ids(i), "v"))
    // Warm up the eviction path, then measure.
    (cap until cap + puts).foreach(i => cache.put(ids(i), "v"))
    val before = threads.getCurrentThreadAllocatedBytes
    (cap + puts until cap + puts * 2).foreach(i => cache.put(ids(i), "v"))
    val after = threads.getCurrentThreadAllocatedBytes
    cache.size shouldBe cap
    cache.stats.evictions shouldBe (puts * 2).toLong
    (after - before) / puts
  }

  "LruStoreCache" should {
    "evict the least recently read or written entries first" in {
      val cache = new LruStoreCache[String](3, None)
      val a = Id[String]("a")
      val b = Id[String]("b")
      val c = Id[String]("c")
      val d = Id[String]("d")
      val e = Id[String]("e")
      cache.put(a, "a")
      cache.put(b, "b")
      cache.put(c, "c")
      cache.get(a) shouldBe Some("a")
      cache.put(d, "d")
      cache.get(b) shouldBe None
      cache.put(c, "c2")
      cache.put(e, "e")
      cache.get(a) shouldBe None
      cache.get(c) shouldBe Some("c2")
      cache.get(d) shouldBe Some("d")
      cache.get(e) shouldBe Some("e")
      cache.size shouldBe 3
      cache.stats.snapshot shouldBe CacheStats.Snapshot(hits = 4, misses = 2, evictions = 2)
    }

    "evict at a cost independent of its size" in {
      val small = bytesPerEvictingPut(100)
      val large = bytesPerEvictingPut(100000)
      withClue(s"bytes allocated per evicting put: cap 100 = $small, cap 100000 = $large: ") {
        large should be <= 1024L
        large should be <= small * 4 + 256
      }
    }
  }
}
