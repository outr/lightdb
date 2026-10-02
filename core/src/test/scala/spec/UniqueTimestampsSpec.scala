package spec

import lightdb.util.{Nowish, UniqueTimestamps}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}
import scala.jdk.CollectionConverters.*

@EmbeddedTest
class UniqueTimestampsSpec extends AnyWordSpec with Matchers {
  "UniqueTimestamps" should {
    "issue strictly increasing stamps faster than the clock moves" in {
      val stamps = new UniqueTimestamps()
      val issued = List.fill(10_000)(stamps.next())
      issued.zip(issued.tail).forall((a, b) => b > a) should be(true)
    }
    "never repeat a stamp across concurrent callers" in {
      val stamps = new UniqueTimestamps()
      val issued = new ConcurrentLinkedQueue[Long]()
      val pool = Executors.newFixedThreadPool(8)
      (1 to 8).foreach(_ => pool.submit((() => (1 to 5_000).foreach(_ => issued.add(stamps.next()))): Runnable))
      pool.shutdown()
      pool.awaitTermination(1, TimeUnit.MINUTES) should be(true)
      issued.asScala.toSet.size should be(40_000)
    }
    "continue past a seed that is ahead of the clock, as after a restart" in {
      val ahead = System.currentTimeMillis() + 3_600_000L
      val restarted = new UniqueTimestamps()
      restarted.seed(ahead)
      restarted.next() should be(ahead + 1)
      restarted.seed(0L)
      restarted.next() should be(ahead + 2)
    }
    "backfill below the first live stamp, counting down, without running ahead of the clock" in {
      val stamps = new UniqueTimestamps()
      val before = System.currentTimeMillis()
      val backfill = stamps.backfill()
      val loaded = List.fill(100_000)(backfill.next())
      loaded.zip(loaded.tail).forall((a, b) => b < a) should be(true)
      loaded.head should be <= System.currentTimeMillis()
      val live = stamps.next()
      live should be > loaded.head
      live should be >= before
      backfill.lowest should be(loaded.last)
    }
    "backfill below an existing collection's earliest stamp" in {
      val stamps = new UniqueTimestamps()
      val backfill = stamps.backfill(below = 1_000L)
      List.fill(3)(backfill.next()) should be(List(999L, 998L, 997L))
    }
    "refuse to backfill into negative stamps" in {
      val backfill = new UniqueTimestamps().backfill(below = 1L)
      backfill.next() should be(0L)
      an[IllegalStateException] should be thrownBy backfill.next()
    }
  }
  "Nowish" should {
    "honour a seed" in {
      // Nowish is process-wide: seed only slightly ahead so other specs in this JVM are unaffected.
      val ahead = Nowish() + 5L
      Nowish.seed(ahead)
      Nowish() should be > ahead
    }
  }
}
