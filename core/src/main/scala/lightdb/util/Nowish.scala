package lightdb.util

import lightdb.time.Timestamp

/**
 * Always returns an incremented timestamp. If called multiple times within the same millisecond, the returned value will
 * be incremented to always be unique.
 *
 * Nowish is a process-wide [[UniqueTimestamps]]. After a restart, [[seed]] it with the largest stamp already persisted so
 * it never reissues one. For a bulk load, use [[backfill]] rather than stamping every record with Nowish, which would
 * carry the stamps ahead of the clock.
 *
 * “Precision isn't always the goal. Uniqueness is, and good enough now is better than fighting the clock.”
 */
object Nowish {
  private val stamps = new UniqueTimestamps()

  def timestamp: Timestamp = Timestamp(apply())

  def apply(): Long = stamps.next()

  /** Ensures every later Nowish value is greater than `atLeast`. */
  def seed(atLeast: Long): Unit = stamps.seed(atLeast)

  /** Stamps that sort before every later Nowish value; see [[UniqueTimestamps.backfill]]. */
  def backfill(below: Long = apply()): UniqueTimestamps.Backfill = stamps.backfill(below)
}
