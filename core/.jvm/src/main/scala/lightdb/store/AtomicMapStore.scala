package lightdb.store

import lightdb.{Id, LightDB}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class AtomicMapStore extends Store {
  private lazy val map = new ConcurrentHashMap[String, Array[Byte]]

  override def keyStream[D]: Iterator[Id[D]] = map.keys().asIterator().asScala.map(Id.apply[D])

  override def stream: Iterator[Array[Byte]] = map.values().iterator().asScala

  override def get[D](id: Id[D]): Option[Array[Byte]] = Option(map.get(id.value))

  override def put[D](id: Id[D], value: Array[Byte]): Boolean = {
    map.put(id.value, value)
    true
  }

  override def delete[D](id: Id[D]): Unit = map.remove(id.value)

  override def count: Int = map.size()

  override def commit(): Unit = ()

  override def dispose(): Unit = map.clear()
}

object AtomicMapStore extends StoreManager {
  override protected def create(db: LightDB, name: String): Store = new AtomicMapStore
}