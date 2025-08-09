package lightdb.rocksdb

import rapid._

import scala.collection.mutable.ArrayBuffer

/**
 * Owns the underlying iterator on a single thread but pulls in batches to amortize hops.
 */
final class ThreadConfinedBufferedIterator[A](mk: => Iterator[A],
                                              name: String = s"rocksdb-iterator-${Unique.withLength(4)()}",
                                              batchSize: Int = 1024) extends Iterator[A] with AutoCloseable {
  require(batchSize > 0)

  private val agent = SingleThreadAgent[Iterator[A]](s"$name-iter")(Task(mk))

  // local batch cache
  private var buf: Vector[A] = Vector.empty[A]
  private var i, len = 0

  private def refill(): Unit = {
    if (i < len) return
    // One hop: pull up to batchSize items on the agent thread
    val v: Vector[A] = agent { it =>
      val out = new ArrayBuffer[A](batchSize)
      var n = 0
      while (n < batchSize && it.hasNext) {
        out += it.next(); n += 1
      }
      out.toVector
    }.sync()
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
    agent {
      case ac: AutoCloseable => ac.close()
      case _ => ()
    }.sync()
    agent.dispose().sync()
  }
}