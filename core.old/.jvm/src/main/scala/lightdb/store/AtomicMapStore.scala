package lightdb.store

import fabric.rw.RW
import lightdb.document.{Document, SetType}
import lightdb.transaction.Transaction
import lightdb.{Id, LightDB}

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

class AtomicMapStore[D <: Document[D]](implicit val rw: RW[D]) extends Store[D] {
  private lazy val map = new ConcurrentHashMap[Id[D], D]

  override def internalCounter: Boolean = true

  override def idIterator(implicit transaction: Transaction[D]): Iterator[Id[D]] = map.keys().asScala

  override def iterator(implicit transaction: Transaction[D]): Iterator[D] = map.values().iterator().asScala

  override def get(id: Id[D])(implicit transaction: Transaction[D]): Option[D] = Option(map.get(id))

  override def contains(id: Id[D])(implicit transaction: Transaction[D]): Boolean = map.containsKey(id)

  override def put(id: Id[D], doc: D)(implicit transaction: Transaction[D]): Option[SetType] = {
    if (map.put(id, doc) != null) {
      Some(SetType.Replace)
    } else {
      Some(SetType.Insert)
    }
  }

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): Boolean = map.remove(id) != null

  override def count(implicit transaction: Transaction[D]): Int = map.size()

  override def commit()(implicit transaction: Transaction[D]): Unit = ()

  override def dispose(): Unit = map.clear()
}

object AtomicMapStore extends StoreManager {
  override protected def create[D <: Document[D]](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = new AtomicMapStore
}