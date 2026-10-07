package lightdb.rocksdb

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

/**
 * The iterators a store has open. A native iterator must be closed before its database is, and on the thread that
 * owns it, so disposal closes every open iterator while its thread is still alive; once closed the registry admits
 * no new ones.
 */
final class RocksDBIteratorRegistry {
  private val open = ConcurrentHashMap.newKeySet[ThreadConfinedBufferedIterator[?]]()
  @volatile private var closed = false

  /** Adds an iterator, or returns false when the registry is closed. */
  def register(iterator: ThreadConfinedBufferedIterator[?]): Boolean = synchronized {
    if closed then false
    else {
      open.add(iterator)
      true
    }
  }

  def unregister(iterator: ThreadConfinedBufferedIterator[?]): Unit = { open.remove(iterator); () }

  /** Closes every open iterator and refuses any later ones. */
  def closeAll(): Unit = {
    val toClose = synchronized {
      closed = true
      open.asScala.toList
    }
    toClose.foreach { it =>
      try it.closeForDispose() catch {
        case t: Throwable => scribe.warn(s"Error closing a RocksDB iterator during dispose: ${t.getMessage}")
      }
    }
    open.clear()
  }
}
