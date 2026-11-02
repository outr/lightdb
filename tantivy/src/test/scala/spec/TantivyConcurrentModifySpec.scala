package spec

import lightdb.store.StoreManager
import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = TantivyStore

  // A Tantivy transaction reads the index as of its last commit.
  override protected def readsOwnWrites: Boolean = false
}
