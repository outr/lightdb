package lightdb.util

import profig.Profig
import fabric.rw.*

import java.util.concurrent.atomic.LongAdder

object StoreMetrics {
  private val enabled: Boolean = Profig("lightdb.metrics.enabled").opt[Boolean].getOrElse(false)
  private val sharedWaitNanos = new LongAdder
  private val sharedAcquireCount = new LongAdder
  private val bufferedFlushCount = new LongAdder
  private val bufferedFlushItems = new LongAdder
  private val bufferedOverflowCount = new LongAdder

  def recordSharedWait(nanos: Long): Unit = if enabled then {
    sharedAcquireCount.increment()
    if nanos > 0 then sharedWaitNanos.add(nanos)
  }

  def recordBufferedFlush(items: Int): Unit = if enabled then {
    bufferedFlushCount.increment()
    bufferedFlushItems.add(items.toLong)
  }

  def recordBufferedOverflow(): Unit = if enabled then bufferedOverflowCount.increment()

  def snapshot(): Snapshot =
    Snapshot(
      sharedAcquireCount = sharedAcquireCount.sum(),
      sharedWaitNanos = sharedWaitNanos.sum(),
      bufferedFlushCount = bufferedFlushCount.sum(),
      bufferedFlushItems = bufferedFlushItems.sum(),
      bufferedOverflowCount = bufferedOverflowCount.sum()
    )

  case class Snapshot(sharedAcquireCount: Long,
                      sharedWaitNanos: Long,
                      bufferedFlushCount: Long,
                      bufferedFlushItems: Long,
                      bufferedOverflowCount: Long)
}
