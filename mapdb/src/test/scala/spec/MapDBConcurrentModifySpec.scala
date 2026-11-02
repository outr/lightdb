package spec

import lightdb.mapdb.MapDBStore
import lightdb.store.StoreManager

@EmbeddedTest
class MapDBConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = MapDBStore

  // Writes go through when made, and a rollback does not undo them.
  override protected def supportsRollback: Boolean = false
}
