package spec

import lightdb.rocksdb.RocksDBStore
import lightdb.store.PrefixScanningStoreManager

@EmbeddedTest
class RocksDBEmployeeInfluenceSpec extends AbstractEmployeeInfluenceSpec {
  override def storeManager: PrefixScanningStoreManager = RocksDBStore
}
