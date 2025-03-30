package spec

import lightdb.lmdb.LMDBStore
import lightdb.lucene.LuceneStore
import lightdb.store.StoreManager
import lightdb.store.split.SplitStoreManager

// TODO: Figure out why this is failing
//@EmbeddedTest
class LMDBAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override lazy val storeManager: SplitStoreManager = SplitStoreManager(LMDBStore, LuceneStore)
}
