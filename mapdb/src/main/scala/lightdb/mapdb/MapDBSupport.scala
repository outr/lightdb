package lightdb.mapdb

import lightdb.{LightDB, Store}

trait MapDBSupport {
  this: LightDB =>

  override protected def createStore(name: String): Store = MapDBStore(Some(directory.resolve(name)))
}