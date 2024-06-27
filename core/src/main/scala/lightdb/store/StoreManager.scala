package lightdb.store

import fabric.rw.RW
import lightdb.LightDB
import lightdb.document.Document

import java.util.concurrent.ConcurrentHashMap

trait StoreManager {
  private val map = new ConcurrentHashMap[String, Store[_]]

  def apply[D <: Document[D]](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = {
    map.computeIfAbsent(name, _ => {
      create(db, name)
    }).asInstanceOf[Store[D]]
  }

  protected def create[D <: Document[D]](db: LightDB, name: String)(implicit rw: RW[D]): Store[D]
}
