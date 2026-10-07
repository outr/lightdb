package lightdb.rocksdb

import lightdb.error.StoreDisposedException
import rapid.*

import scala.collection.mutable.ArrayBuffer

/**
 * Owns the underlying iterator on a single thread but pulls in batches to amortize hops.
 *
 * With a `guard`, each batch runs as a native call of that guard, so it is refused once the database behind it is
 * closing. Closing is idempotent; an iterator closed by its store's disposal refuses further reads with a
 * [[StoreDisposedException]], one closed by its user simply ends.
 */
final class ThreadConfinedBufferedIterator[A](mk: => Iterator[A],
                                              name: String = s"rocksdb-iterator-${Unique.withLength(4)()}",
                                              batchSize: Int = 1024,
                                              sharedAgent: Option[SingleThreadAgent[Unit]] = None,
                                              onClose: () => Unit = () => (),
                                              guard: Option[RocksDBNativeGuard] = None,
                                              storeName: String = "rocksdb") extends Iterator[A] with AutoCloseable {
  require(batchSize > 0)

  // If a shared agent is provided (ex: transaction-scoped), we reuse it to avoid creating a new OS thread per iterator.
  // Otherwise, preserve the previous behavior: one agent (and one dedicated OS thread) per iterator.
  private val localAgent: SingleThreadAgent[Iterator[A]] =
    if sharedAgent.isEmpty then SingleThreadAgent[Iterator[A]](s"$name-iter")(Task(mk)) else null

  // Under shared agent mode, we create the underlying iterator on the agent thread lazily and keep it here.
  private var sharedIt: Iterator[A] = _

  @volatile private var closed = false
  @volatile private var closedByDispose = false

  private def onAgent[Return](f: Iterator[A] => Return): Return = {
    sharedAgent match {
      case Some(agent) =>
        agent { _ =>
          if sharedIt == null && !closed then sharedIt = mk
          f(sharedIt)
        }.sync()
      case None =>
        localAgent(f).sync()
    }
  }

  private def native[Return](f: => Return): Return = guard match {
    case Some(g) => g(storeName)(f)
    case None => f
  }

  private def ensureOpen(): Unit = if closedByDispose then throw StoreDisposedException(storeName)

  // local batch cache
  private var buf: Vector[A] = Vector.empty[A]
  private var i, len = 0

  private def refill(): Unit = {
    if i < len then ()
    else if closed then {
      ensureOpen()
      buf = Vector.empty
      i = 0
      len = 0
    } else {
      // One hop: pull up to batchSize items on the agent thread
      val v: Vector[A] = native(onAgent { it =>
        val out = new ArrayBuffer[A](batchSize)
        // A close queued on the agent ahead of this batch has released the native iterator.
        if closed then ensureOpen()
        else {
          var n = 0
          while n < batchSize && it.hasNext do {
            out += it.next(); n += 1
          }
        }
        out.toVector
      })
      buf = v
      i = 0
      len = buf.length
    }
  }

  override def hasNext: Boolean = {
    refill()
    i < len
  }

  override def next(): A = {
    if !hasNext then throw new NoSuchElementException("exhausted")
    val a = buf(i)
    i += 1
    a
  }

  /** Closes the iterator on behalf of its store's disposal: later reads are refused rather than ending quietly. */
  def closeForDispose(): Unit = {
    closedByDispose = true
    close()
  }

  override def close(): Unit = synchronized {
    if !closed then {
      closed = true
      try {
        sharedAgent match {
          case Some(agent) =>
            // Close the underlying iterator on the agent thread, but do NOT dispose the shared agent.
            agent { _ =>
              val it = sharedIt
              sharedIt = null
              it match {
                case ac: AutoCloseable => ac.close()
                case _ => ()
              }
            }.sync()
          case None =>
            localAgent {
              case ac: AutoCloseable => ac.close()
              case _ => ()
            }.sync()
            localAgent.dispose().sync()
        }
      } finally {
        onClose()
      }
    }
  }
}
