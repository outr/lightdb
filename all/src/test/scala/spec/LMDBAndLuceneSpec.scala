package spec

import lightdb.lmdb.LMDBStore
import lightdb.lucene.LuceneStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class LMDBAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override lazy val storeManager: SplitStoreManager[LMDBStore.type, LuceneStore.type] = SplitStoreManager(LMDBStore, LuceneStore)
}
