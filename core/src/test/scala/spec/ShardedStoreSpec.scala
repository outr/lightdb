package spec

import lightdb.store.{MapStore, StoreManager}
import lightdb.store.sharded.ShardedStoreManager

@EmbeddedTest
class ShardedStoreSpec extends AbstractKeyValueSpec {
  override def storeManager: StoreManager = ShardedStoreManager(MapStore, 3)
}