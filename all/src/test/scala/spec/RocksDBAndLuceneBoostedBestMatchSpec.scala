package spec

import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class RocksDBAndLuceneBoostedBestMatchSpec extends AbstractBoostedBestMatchSpec {
  override def storeManager: SplitStoreManager[RocksDBStore.type, LuceneStore.type] = SplitStoreManager(RocksDBStore, LuceneStore)
}
