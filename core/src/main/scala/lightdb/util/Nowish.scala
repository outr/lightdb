package lightdb.util

import java.util.concurrent.atomic.AtomicLong

/**
 * Always returns an incremented timestamp. If called multiple times within the same millisecond, the returned value will
 * be incremented to always be unique.
 *
 * “Precision isn't always the goal. Uniqueness is, and good enough now is better than fighting the clock.”
 */
object Nowish {
  private val lastTime = new AtomicLong(-1L)

  def apply(): Long = lastTime.updateAndGet { last =>
    val now = System.currentTimeMillis()
    if now > last then {
      now
    } else {
      last + 1
    }
  }
}