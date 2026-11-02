package spec

import lightdb.halodb.HaloDBStore
import lightdb.store.StoreManager

@EmbeddedTest
class HaloDBConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = HaloDBStore

  // Writes go through when made, and a rollback does not undo them.
  override protected def supportsRollback: Boolean = false
}
