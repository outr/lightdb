package lightdb.traversal.store

import profig.Profig
import fabric.rw.*

import java.util.concurrent.atomic.LongAdder

object TraversalMetrics {
  private def enabled: Boolean = Profig("lightdb.traversal.metrics.enabled").opt[Boolean].getOrElse(false)
  private val queryCount = new LongAdder
  private val queryNanos = new LongAdder
  private val scanFallbackCount = new LongAdder

  def recordQueryDuration(nanos: Long): Unit = if enabled then {
    queryCount.increment()
    queryNanos.add(nanos)
  }

  def recordScanFallback(): Unit = if enabled then scanFallbackCount.increment()

  def snapshot(): Snapshot =
    Snapshot(
      queryCount = queryCount.sum(),
      queryNanos = queryNanos.sum(),
      scanFallbackCount = scanFallbackCount.sum()
    )

  case class Snapshot(queryCount: Long,
                      queryNanos: Long,
                      scanFallbackCount: Long)
}
