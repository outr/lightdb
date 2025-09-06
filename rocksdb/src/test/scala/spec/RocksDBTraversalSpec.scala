package spec

import lightdb.rocksdb.RocksDBStore
import lightdb.store.prefix.PrefixScanningStoreManager

@EmbeddedTest
class RocksDBTraversalSpec extends AbstractTraversalSpec {
  override def storeManager: PrefixScanningStoreManager = RocksDBStore
}
