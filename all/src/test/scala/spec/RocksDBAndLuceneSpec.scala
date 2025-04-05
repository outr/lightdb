package spec

import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class RocksDBAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override def storeManager: SplitStoreManager = SplitStoreManager(RocksDBStore, LuceneStore)
}
