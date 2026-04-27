package spec

import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class RocksDBAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  // Tie-break ordering is implementation-defined under Lucene strict-insert NRT probing.
  override protected def scoredResultsOrderingSupported: Boolean = false

  override def storeManager: SplitStoreManager[RocksDBStore.type, LuceneStore.type] = SplitStoreManager(RocksDBStore, LuceneStore)
}
