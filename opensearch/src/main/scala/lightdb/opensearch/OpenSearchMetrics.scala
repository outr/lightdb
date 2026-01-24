package lightdb.opensearch

import rapid.Task

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.duration.{DurationLong, FiniteDuration}

object OpenSearchMetrics {
  case class Snapshot(key: String,
                      requests: Long,
                      failures: Long,
                      retries: Long,
                      latencyTotalMs: Long,
                      latencyMaxMs: Long,
                      bulkDocs: Long,
                      bulkBytes: Long) {
    def avgLatencyMs: Double = if requests <= 0 then 0.0 else latencyTotalMs.toDouble / requests.toDouble
  }

  private case class State(requests: AtomicLong,
                           failures: AtomicLong,
                           retries: AtomicLong,
                           latencyTotalMs: AtomicLong,
                           latencyMaxMs: AtomicLong,
                           bulkDocs: AtomicLong,
                           bulkBytes: AtomicLong,
                           logStarted: AtomicBoolean)

  private val byKey = new ConcurrentHashMap[String, State]()

  private def state(key: String): State = byKey.computeIfAbsent(key, _ => State(
    requests = new AtomicLong(0L),
    failures = new AtomicLong(0L),
    retries = new AtomicLong(0L),
    latencyTotalMs = new AtomicLong(0L),
    latencyMaxMs = new AtomicLong(0L),
    bulkDocs = new AtomicLong(0L),
    bulkBytes = new AtomicLong(0L),
    logStarted = new AtomicBoolean(false)
  ))

  private def updateMax(max: AtomicLong, value: Long): Unit = {
    var done = false
    while !done do {
      val current = max.get()
      if value > current then done = max.compareAndSet(current, value)
      else done = true
    }
  }

  def recordRequest(key: String, tookMs: Long): Unit = {
    val s = state(key)
    s.requests.incrementAndGet()
    s.latencyTotalMs.addAndGet(tookMs)
    updateMax(s.latencyMaxMs, tookMs)
  }

  def recordFailure(key: String): Unit =
    state(key).failures.incrementAndGet()

  def recordRetry(key: String): Unit =
    state(key).retries.incrementAndGet()

  def recordBulkAttempt(key: String, docs: Int, bytes: Int): Unit = {
    val s = state(key)
    s.bulkDocs.addAndGet(docs.toLong)
    s.bulkBytes.addAndGet(bytes.toLong)
  }

  def snapshot(key: String): Snapshot = {
    val s = state(key)
    Snapshot(
      key = key,
      requests = s.requests.get(),
      failures = s.failures.get(),
      retries = s.retries.get(),
      latencyTotalMs = s.latencyTotalMs.get(),
      latencyMaxMs = s.latencyMaxMs.get(),
      bulkDocs = s.bulkDocs.get(),
      bulkBytes = s.bulkBytes.get()
    )
  }

  def startPeriodicLogging(key: String, every: FiniteDuration): Unit = {
    val s = state(key)
    if s.logStarted.compareAndSet(false, true) then {
      def loop(): Task[Unit] =
        Task.sleep(every).next {
          val snap = snapshot(key)
          scribe.info(
            s"OpenSearchMetrics($key): requests=${snap.requests} failures=${snap.failures} retries=${snap.retries} " +
              f"avgLatencyMs=${snap.avgLatencyMs}%.2f maxLatencyMs=${snap.latencyMaxMs} " +
              s"bulkDocs=${snap.bulkDocs} bulkBytes=${snap.bulkBytes}"
          )
          loop()
        }
      loop().start()
    }
  }
}


