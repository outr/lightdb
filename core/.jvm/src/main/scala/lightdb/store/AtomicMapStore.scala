package lightdb.store

import fabric.rw.RW
import lightdb.{Id, LightDB}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class AtomicMapStore[D](implicit val rw: RW[D]) extends Store[D] {
  private lazy val map = new ConcurrentHashMap[Id[D], D]

  override def idIterator: Iterator[Id[D]] = map.keys().asScala

  override def iterator: Iterator[D] = map.values().iterator().asScala

  override def get(id: Id[D]): Option[D] = Option(map.get(id))

  override def put(id: Id[D], doc: D): Boolean = {
    map.put(id, doc)
    true
  }

  override def delete(id: Id[D]): Unit = map.remove(id)

  override def count: Int = map.size()

  override def commit(): Unit = ()

  override def dispose(): Unit = map.clear()
}

object AtomicMapStore extends StoreManager {
  override protected def create[D](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new AtomicMapStore
}