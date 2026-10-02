package lightdb.util

import lightdb.time.Timestamp

import java.util.concurrent.atomic.AtomicLong

/**
 * A source of unique, strictly increasing epoch-millisecond stamps: the current time, or one past the last stamp when
 * the clock has not moved on. Because no two stamps are equal, a reader that has seen every record up to a stamp can
 * resume with `since = thatStamp` and needs no cursor to break ties.
 *
 * Unlike a bare counter it can be [[seed]]ed, so a process that restarts after a burst carried it ahead of the clock
 * continues past its last stamp instead of reissuing or going backwards. Seed it with the largest stamp already
 * persisted before issuing any.
 *
 * Bulk loads should not use [[next]]: at one stamp per millisecond, writing ten million records would carry the stamps
 * hours into the future. Use [[backfill]], which counts down from below the earliest stamp instead.
 *
 * Uniqueness holds within one instance. Stamps from separate processes, or separate instances, can collide; give one
 * writer the job of stamping a given collection.
 */
final class UniqueTimestamps(initial: Long = -1L) {
  private val last = new AtomicLong(initial)

  /** The next stamp: now, or one past the previous stamp if now is not later. */
  def next(): Long = last.updateAndGet { previous =>
    val now = System.currentTimeMillis()
    if now > previous then now else previous + 1
  }

  def nextTimestamp: Timestamp = Timestamp(next())

  /** Ensures every later stamp is greater than `atLeast`; a lower value changes nothing. */
  def seed(atLeast: Long): Unit = last.accumulateAndGet(atLeast, math.max)

  /** The last stamp issued or seeded, -1 before any. */
  def lastIssued: Long = last.get()

  /**
   * Stamps for a bulk load that sort before everything this instance issues from now on: `below - 1`, `below - 2`, ...
   * counting down, so the load never runs ahead of the clock however large it is. `below` defaults to a fresh stamp,
   * which also seeds this instance past it. For a collection that already holds stamped records, pass its smallest
   * stamp so the backfill sorts before those too.
   */
  def backfill(below: Long = next()): UniqueTimestamps.Backfill = {
    seed(below)
    new UniqueTimestamps.Backfill(below)
  }
}

object UniqueTimestamps {
  /** A countdown of unique stamps, each smaller than the last. See [[UniqueTimestamps.backfill]]. */
  final class Backfill private[UniqueTimestamps](below: Long) {
    private val last = new AtomicLong(below)

    def next(): Long = {
      val stamp = last.decrementAndGet()
      if stamp < 0L then throw new IllegalStateException(s"Backfill below $below ran out of non-negative stamps")
      stamp
    }

    def nextTimestamp: Timestamp = Timestamp(next())

    /** The smallest stamp issued so far, or `below` before any. */
    def lowest: Long = last.get()
  }
}
