package lightdb.store.journal

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

/** How a journal retries applying a committed intent: `attempts` tries, the delay doubling from `initialDelay` up to
  * `maxDelay`. */
case class JournalRetry(attempts: Int = 6, initialDelay: FiniteDuration = 50.millis, maxDelay: FiniteDuration = 2.seconds) {
  require(attempts >= 1, s"attempts must be at least 1, not $attempts")

  def delay(attempt: Int): FiniteDuration = {
    val millis = initialDelay.toMillis * (1L << math.min(attempt - 1, 20))
    math.min(millis, maxDelay.toMillis).millis
  }
}
