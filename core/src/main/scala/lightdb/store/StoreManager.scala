package lightdb.store

import fabric.rw.RW
import lightdb.LightDB

import java.util.concurrent.ConcurrentHashMap

trait StoreManager {
  private val map = new ConcurrentHashMap[String, Store[_]]

  def apply[D](db: LightDB, name: String)(implicit rw: RW[D]): Store[D] = {
    map.computeIfAbsent(name, _ => {
      create(db, name)
    }).asInstanceOf[Store[D]]
  }

  protected def create[D](db: LightDB, name: String)(implicit rw: RW[D]): Store[D]
}
