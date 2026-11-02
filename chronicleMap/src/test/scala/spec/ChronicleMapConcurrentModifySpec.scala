package spec

import lightdb.chroniclemap.ChronicleMapStore
import lightdb.store.StoreManager

@EmbeddedTest
class ChronicleMapConcurrentModifySpec extends AbstractConcurrentModifySpec with ChronicleMapAvailability {
  override def storeManager: StoreManager = ChronicleMapStore

  // Writes go through when made, and a rollback does not undo them.
  override protected def supportsRollback: Boolean = false
}
