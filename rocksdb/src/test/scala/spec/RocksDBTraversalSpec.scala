package spec

import lightdb.rocksdb.RocksDBStore
import lightdb.store.PrefixScanningStoreManager

@EmbeddedTest
class RocksDBTraversalSpec extends AbstractTraversalSpec {
  override def storeManager: PrefixScanningStoreManager = RocksDBStore
}
