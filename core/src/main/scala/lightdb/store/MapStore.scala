package lightdb.store

import fabric.rw.RW
import lightdb.document.SetType
import lightdb.{Id, LightDB}

/**
 * Simple in-memory Store backed by Map.
 *
 * Note: It is recommended to use AtomicMapStore on the JVM as a more efficient alternative to this.
 */
 class MapStore[D](implicit val rw: RW[D]) extends Store[D] {
  private var map = Map.empty[Id[D], D]

  override def internalCounter: Boolean = true

  override def idIterator: Iterator[Id[D]] = map.keys.iterator

  override def iterator: Iterator[D] = map.values.iterator

  override def get(id: Id[D]): Option[D] = map.get(id)

  override def contains(id: Id[D]): Boolean = map.contains(id)

  override def put(id: Id[D], doc: D): Option[SetType] = synchronized {
    val `type` = if (contains(id)) {
      SetType.Replace
    } else {
      SetType.Insert
    }
    map += id -> doc
    Some(`type`)
  }

  override def delete(id: Id[D]): Boolean = synchronized {
    val exists = contains(id)
    map -= id
    exists
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