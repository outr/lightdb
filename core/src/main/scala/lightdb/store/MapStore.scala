package lightdb.store

import fabric.rw.RW
import lightdb.{Id, LightDB}

/**
 * Simple in-memory Store backed by Map.
 *
 * Note: It is recommended to use AtomicMapStore on the JVM as a more efficient alternative to this.
 */
 class MapStore[D](implicit val rw: RW[D]) extends Store[D] {
  private var map = Map.empty[Id[D], D]

  override def idIterator: Iterator[Id[D]] = map.keys.iterator

  override def iterator: Iterator[D] = map.values.iterator

  override def get(id: Id[D]): Option[D] = map.get(id)

  override def put(id: Id[D], doc: D): Boolean = synchronized {
    map += id -> doc
    true
  }

  override def delete(id: Id[D]): Unit = synchronized {
    map -= id
  }

  override def count: Int = map.size

  override def commit(): Unit = ()

  override def dispose(): Unit = synchronized {
    map = Map.empty
  }
}

object MapStore extends StoreManager {
  override protected def create[D](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new MapStore
}