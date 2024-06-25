package lightdb.store

import cats.effect.IO
import fabric.rw.RW
import lightdb.LightDB
import lightdb.document.Document

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
