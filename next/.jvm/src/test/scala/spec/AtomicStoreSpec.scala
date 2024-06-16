package spec
import lightdb.store.{AtomicMapStore, StoreManager}

class AtomicStoreSpec extends AbstractStoreSpec {
  override protected def storeManager: StoreManager = AtomicMapStore
}
