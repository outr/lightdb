package spec

import lightdb.lucene.LuceneStore
import lightdb.store.StoreManager
import lightdb.store.sharded.ShardedStoreManager

@EmbeddedTest
class LuceneShardedSpec extends AbstractBasicSpec {
  override def storeManager: StoreManager = ShardedStoreManager(LuceneStore, 6)
}
