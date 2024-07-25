package lightdb.util

import java.util.concurrent.atomic.AtomicLong

/**
 * Always returns an incremented timestamp. If called multiple times within the same millisecond, the returned value will
 * be incremented to always be unique.
 */
object Nowish {
  private val lastTime = new AtomicLong(-1L)

  def apply(): Long = lastTime.updateAndGet(last => {
    val now = System.currentTimeMillis()
    if (now > last) {
      now
    } else {
      last + 1
    }
  })
}