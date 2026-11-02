package spec

import lightdb.lucene.LuceneStore
import lightdb.store.StoreManager

@EmbeddedTest
class LuceneConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = LuceneStore

  // A Lucene transaction reads the index as of its last commit.
  override protected def readsOwnWrites: Boolean = false
}
