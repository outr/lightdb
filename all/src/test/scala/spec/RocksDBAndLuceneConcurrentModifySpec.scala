package spec

import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.store.StoreManager
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class RocksDBAndLuceneConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = SplitStoreManager(RocksDBStore, LuceneStore)
}
