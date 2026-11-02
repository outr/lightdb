package spec

import lightdb.rocksdb.RocksDBStore
import lightdb.store.StoreManager

@EmbeddedTest
class RocksDBConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = RocksDBStore
}
