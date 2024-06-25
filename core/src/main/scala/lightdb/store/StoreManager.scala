package lightdb.store

import lightdb.LightDB

import java.util.concurrent.ConcurrentHashMap

trait StoreManager {
  private val map = new ConcurrentHashMap[String, Store]

  def apply(db: LightDB, name: String): Store = {
    map.computeIfAbsent(name, _ => {
      create(db, name)
    })
  }

  protected def create(db: LightDB, name: String): Store
}
