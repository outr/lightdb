package spec

import lightdb.rocksdb.RocksDBStore
import lightdb.store.prefix.PrefixScanningStoreManager

@EmbeddedTest
class RocksDBDeliveryPathSpec extends AbstractDeliveryPathSpec {
  override def storeManager: PrefixScanningStoreManager = RocksDBStore
}
