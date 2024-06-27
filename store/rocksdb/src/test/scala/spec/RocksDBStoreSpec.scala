package spec

import lightdb.rocks.RocksDBStore
import lightdb.store.StoreManager

class RocksDBStoreSpec extends AbstractStoreSpec {
  override protected def storeManager: StoreManager = RocksDBStore
}
