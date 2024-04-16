package lightdb.rocks

import lightdb.{LightDB, Store}

trait RocksDBSupport {
  this: LightDB =>

  override protected def createStore(name: String): Store = RocksDBStore(directory.resolve(name))
}
