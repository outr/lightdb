package spec

import lightdb.h2.H2Store
import lightdb.store.CollectionManager
import lightdb.store.hashmap.HashMapStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class H2AndMapSplitSpec extends AbstractBasicSpec {
  override protected def memoryOnly: Boolean = true

  override def storeManager: CollectionManager = SplitStoreManager(HashMapStore, H2Store)
}