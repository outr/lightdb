package spec

import lightdb.lmdb.LMDBStore
import lightdb.lucene.LuceneStore
import lightdb.store.split.SplitStoreManager

// TODO: Figure out why this inconsistently works
//@EmbeddedTest
class LMDBAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override lazy val storeManager: SplitStoreManager[LMDBStore.type, LuceneStore.type] = SplitStoreManager(LMDBStore, LuceneStore)
}
