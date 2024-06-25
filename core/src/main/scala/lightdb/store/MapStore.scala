package lightdb.store

import lightdb.{Id, LightDB}

/**
 * Simple in-memory Store backed by Map.
 *
 * Note: It is recommended to use AtomicMapStore on the JVM as a more efficient alternative to this.
 */
class MapStore extends Store { store =>
  private var map = Map.empty[String, Array[Byte]]

  override def keyStream[D]: Iterator[Id[D]] = map.keys.iterator.map(Id.apply[D])

  override def stream: Iterator[Array[Byte]] = map.values.iterator

  override def get[D](id: Id[D]): Option[Array[Byte]] = map.get(id.value)

  override def put[D](id: Id[D], value: Array[Byte]): Boolean = {
    store.synchronized {
      map += id.value -> value
    }
    true
  }

  override def delete[D](id: Id[D]): Unit = {
    store.synchronized {
      map -= id.value
    }
  }

  override def count: Int = map.size

  override def commit(): Unit = ()

  override def dispose(): Unit = store.synchronized {
    map = Map.empty
  }
}

object MapStore extends StoreManager {
  override protected def create(db: LightDB, name: String): Store = new MapStore
}