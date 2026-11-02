package spec

import lightdb.lmdb.LMDBStore
import lightdb.store.StoreManager

@EmbeddedTest
class LMDBConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = LMDBStore
}
