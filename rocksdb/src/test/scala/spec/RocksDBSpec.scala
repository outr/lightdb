package spec

import lightdb.rocksdb.RocksDBStore
import lightdb.store.StoreManager

//@EmbeddedTest
class RocksDBSpec extends AbstractKeyValueSpec {
  override def storeManager: StoreManager = RocksDBStore
}
