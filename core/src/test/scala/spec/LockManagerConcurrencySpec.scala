package spec

import lightdb.lock.LockManager
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.*

/**
 * Coverage for the eviction TOCTOU race in [[LockManager.release]].
 *
 * Before the fix, `release` decided whether to evict the entry from
 * `locks` by reading `availablePermits()` AFTER calling
 * `lock.release()`. That left a window where a queued waiter unblocked
 * by `release()` could grab the permit (permits→0) right between the
 * two operations, but only sometimes — so the eviction predicate often
 * fired the wrong way:
 *
 *   - Holder T_A releases → waiter T_B unblocks but is still
 *     transitioning to RUNNING.
 *   - T_A reads `availablePermits` → still 1 → entry removed.
 *   - T_B's `acquire()` now returns; T_B does its work; T_B calls
 *     `release(K)` → `existingLock = null` → NPE in the `compute`
 *     lambda.
 *
 * The test runs many parallel acquire+release cycles on the same key
 * to exercise the race. Pre-fix this spec NPEs deterministically
 * within a few iterations; post-fix it stays clean.
 */
@EmbeddedTest
class LockManagerConcurrencySpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {

  "LockManager.release under concurrent acquire/release on the same key" should {

    "never NPE and never let two holders run their critical sections concurrently" in {
      val lm = new LockManager[String, Int]
      val inSection      = new AtomicInteger(0)
      val maxConcurrency = new AtomicInteger(0)
      val key            = "K"
      val failure        = new AtomicReference[Throwable](null)

      val workers = 32
      val cyclesPerWorker = 200

      def acquireReleaseCycle: Task[Unit] = lm.apply(
        key = key,
        value = Task.pure(Option(0))
      ) { _ =>
        Task {
          val n = inSection.incrementAndGet()
          // Track the maximum number of concurrent in-section holders;
          // the LockManager invariant says this should stay at 1.
          maxConcurrency.updateAndGet(m => if (n > m) n else m)
          // Tiny spin so the holder really overlaps the next acquirer
          // and the race window is exercised.
          val start = System.nanoTime()
          while (System.nanoTime() - start < 5_000L) ()
          inSection.decrementAndGet()
          Option(n)
        }
      }.unit.handleError { t =>
        failure.compareAndSet(null, t)
        Task.unit
      }

      val all = (1 to workers).toList.map(_ =>
        (1 to cyclesPerWorker).toList.foldLeft(Task.unit)((acc, _) => acc.flatMap(_ => acquireReleaseCycle))
      )
      Task.parSequence(all).map { _ =>
        Option(failure.get()) match {
          case Some(t) => fail(s"LockManager raised under contention: ${t.getClass.getName}: ${t.getMessage}", t)
          case None    => maxConcurrency.get shouldBe 1
        }
      }
    }
  }
}
