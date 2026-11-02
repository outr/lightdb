package spec

import lightdb.store.StoreManager

@EmbeddedTest
class RocksDBTraversalStoreConcurrentModifySpec extends AbstractConcurrentModifySpec with TraversalRocksDBWrappedManager {
  override def storeManager: StoreManager = traversalStoreManager

  // Writes go through when made, and a rollback does not undo them.
  override protected def supportsRollback: Boolean = false
}
