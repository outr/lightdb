package spec

import lightdb.opensearch.OpenSearchStore
import lightdb.store.CollectionManager
import lightdb.store.hashmap.HashMapStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class MapAndOpenSearchSplitSpec extends AbstractBasicSpec with OpenSearchTestSupport {
  override protected def memoryOnly: Boolean = true
  override protected def readYourWritesWithinTransactionSupported: Boolean = false

  override def storeManager: CollectionManager = SplitStoreManager(HashMapStore, OpenSearchStore)
}


