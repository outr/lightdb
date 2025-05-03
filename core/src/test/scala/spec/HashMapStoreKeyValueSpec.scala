package spec

import lightdb.store.StoreManager
import lightdb.store.hashmap.HashMapStore

@EmbeddedTest
class HashMapStoreKeyValueSpec extends AbstractKeyValueSpec {
  override def storeManager: StoreManager = HashMapStore
}