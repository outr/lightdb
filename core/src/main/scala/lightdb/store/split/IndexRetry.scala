package lightdb.store.split

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

/** How a split collection re-indexes rows whose index update failed after their storage write: `attempts` passes by
  * id, the delay doubling from `initialDelay` up to `maxDelay`, then one full re-index before the index is reported
  * failed. */
case class IndexRetry(attempts: Int = 6, initialDelay: FiniteDuration = 100.millis, maxDelay: FiniteDuration = 5.seconds) {
  require(attempts >= 1, s"attempts must be at least 1, not $attempts")

  def delay(attempt: Int): FiniteDuration = {
    val millis = initialDelay.toMillis * (1L << math.min(attempt - 1, 20))
    math.min(millis, maxDelay.toMillis).millis
  }
}
