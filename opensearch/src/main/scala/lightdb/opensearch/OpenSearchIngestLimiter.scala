package lightdb.opensearch

import rapid.Task

import java.util.concurrent.{ConcurrentHashMap, Semaphore}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Shared ingest limiter to avoid overwhelming an OpenSearch cluster when many transactions commit concurrently.
 *
 * Keys are typically config.normalizedBaseUrl (i.e. one limiter per cluster endpoint).
 */
object OpenSearchIngestLimiter {
  private case class State(sem: Semaphore, active: AtomicInteger)

  private val limiters = new ConcurrentHashMap[String, State]()

  private def stateFor(key: String, maxConcurrent: Int): State = {
    // Update-on-change is intentionally not supported: treat concurrency as a stable process-wide setting.
    limiters.computeIfAbsent(key, _ => State(new Semaphore(maxConcurrent, true), new AtomicInteger(0)))
  }

  def withPermit[A](key: String, maxConcurrent: Int)(task: => Task[A]): Task[A] = {
    if maxConcurrent == Int.MaxValue then {
      task
    } else {
      val st = stateFor(key, maxConcurrent)
      Task(st.sem.acquire()).flatMap { _ =>
        st.active.incrementAndGet()
        task.guarantee(Task {
          st.active.decrementAndGet()
          st.sem.release()
        })
      }
    }
  }

  private[opensearch] def active(key: String): Int =
    Option(limiters.get(key)).map(_.active.get()).getOrElse(0)
}


