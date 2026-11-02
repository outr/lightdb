package spec

import lightdb.store.StoreManager
import lightdb.store.hashmap.HashMapStore

@EmbeddedTest
class HashMapStoreConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = HashMapStore

  // Writes go through when made, and a rollback does not undo them.
  override protected def supportsRollback: Boolean = false
}
