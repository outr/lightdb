package lightdb.transaction.batch

import scala.concurrent.duration.{DurationInt, FiniteDuration}

sealed trait BatchConfig

object BatchConfig {
  /** Immediate writes - no batching */
  case object Direct extends BatchConfig

  /** Buffered writes with coalescing and read-your-writes.
    * Uses Map[Id, WriteOp] - multiple writes to same ID are merged.
    */
  case class Buffered(maxBufferSize: Int = 20_000) extends BatchConfig

  /** Queue writes, flush on overflow or commit.
    * Order-preserving, no read-your-writes.
    */
  case class Queued(maxQueueSize: Int = 5_000) extends BatchConfig

  /** Async background processing with worker threads. */
  case class Async(
      activeThreads: Int = 4,
      chunkSize: Int = 5_000,
      waitTime: FiniteDuration = 250.millis,
      maxQueueSize: Int = 20_000
  ) extends BatchConfig

  /** Defer to store's native batch implementation. */
  case object StoreNative extends BatchConfig
}
