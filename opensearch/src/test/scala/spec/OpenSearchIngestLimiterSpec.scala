package spec

import lightdb.opensearch.OpenSearchIngestLimiter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}
import rapid.taskSeq2Ops

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationLong

class OpenSearchIngestLimiterSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  "OpenSearchIngestLimiter" should {
    "enforce max concurrent permits" in {
      val key = "http://example"
      val max = 2
      val active = new AtomicInteger(0)
      val peak = new AtomicInteger(0)

      def updatePeak(v: Int): Unit = {
        var done = false
        while !done do {
          val p = peak.get()
          if v > p then done = peak.compareAndSet(p, v) else done = true
        }
      }

      val tasks = (1 to 12).toList.map { _ =>
        OpenSearchIngestLimiter.withPermit(key, max) {
          Task {
            val now = active.incrementAndGet()
            updatePeak(now)
          }.next(Task.sleep(50.millis)).guarantee(Task(active.decrementAndGet())).unit
        }
      }

      tasks.tasksPar.map { _ =>
        peak.get() should be <= max
      }
    }
  }
}


