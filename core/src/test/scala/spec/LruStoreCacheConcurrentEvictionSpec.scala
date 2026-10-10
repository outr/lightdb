package spec

import lightdb.cache.LruStoreCache
import lightdb.id.Id
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicBoolean

/**
 * An LRU store cache over its cap evicts by access time while readers keep touching entries: every
 * eviction must complete and leave the cache at its cap. Sorting by an access time read live fails
 * here with "Comparison method violates its general contract!", since the readers change it mid-sort.
 */
class LruStoreCacheConcurrentEvictionSpec extends AnyWordSpec with Matchers {
  "LruStoreCache" should {
    "evict under its cap while readers touch entries" in {
      val cap = 5000
      val cache = new LruStoreCache[String](cap, None)
      (0 until cap).foreach(i => cache.put(Id[String](s"id-$i"), s"v$i"))
      val stop = new AtomicBoolean(false)
      val readers = (0 until 4).map { r =>
        val t = new Thread(() => {
          val random = new scala.util.Random(r)
          while (!stop.get()) cache.get(Id[String](s"id-${random.nextInt(cap)}"))
        })
        t.setDaemon(true)
        t.start()
        t
      }
      val failures = try (0 until 200).flatMap { i =>
        try {
          cache.put(Id[String](s"new-$i"), s"n$i")
          None
        } catch {
          case t: IllegalArgumentException => Some(t.getMessage)
        }
      } finally {
        stop.set(true)
        readers.foreach(_.join())
      }
      withClue(failures.headOption.getOrElse("")) {
        failures.size shouldBe 0
      }
      cache.size shouldBe cap
    }
  }
}
