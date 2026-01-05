package lightdb.rocksdb

import rapid._

import scala.collection.mutable.ArrayBuffer

/**
 * Owns the underlying iterator on a single thread but pulls in batches to amortize hops.
 */
final class ThreadConfinedBufferedIterator[A](mk: => Iterator[A],
                                              name: String = s"rocksdb-iterator-${Unique.withLength(4)()}",
                                              batchSize: Int = 1024,
                                              sharedAgent: Option[SingleThreadAgent[Unit]] = None,
                                              onClose: () => Unit = () => ()) extends Iterator[A] with AutoCloseable {
  require(batchSize > 0)

  // If a shared agent is provided (ex: transaction-scoped), we reuse it to avoid creating a new OS thread per iterator.
  // Otherwise, preserve the previous behavior: one agent (and one dedicated OS thread) per iterator.
  private val localAgent: SingleThreadAgent[Iterator[A]] =
    if (sharedAgent.isEmpty) SingleThreadAgent[Iterator[A]](s"$name-iter")(Task(mk)) else null

  // Under shared agent mode, we create the underlying iterator on the agent thread lazily and keep it here.
  private var sharedIt: Iterator[A] = _

  private def onAgent[Return](f: Iterator[A] => Return): Return = {
    sharedAgent match {
      case Some(agent) =>
        agent { _ =>
          if (sharedIt == null) sharedIt = mk
          f(sharedIt)
        }.sync()
      case None =>
        localAgent(f).sync()
    }
  }

  // local batch cache
  private var buf: Vector[A] = Vector.empty[A]
  private var i, len = 0

  private def refill(): Unit = {
    if (i < len) return
    // One hop: pull up to batchSize items on the agent thread
    val v: Vector[A] = onAgent { it =>
      val out = new ArrayBuffer[A](batchSize)
      var n = 0
      while (n < batchSize && it.hasNext) {
        out += it.next(); n += 1
      }
      out.toVector
    }
    buf = v
    i = 0
    len = buf.length
  }

  override def hasNext: Boolean = {
    refill()
    i < len
  }

  override def next(): A = {
    if (!hasNext) throw new NoSuchElementException("exhausted")
    val a = buf(i)
    i += 1
    a
  }

  override def close(): Unit = {
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