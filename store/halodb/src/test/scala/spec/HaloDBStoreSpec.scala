package spec
import lightdb.halo.HaloDBStore
import lightdb.store.StoreManager

class HaloDBStoreSpec extends AbstractStoreSpec {
  override protected def storeManager: StoreManager = HaloDBStore
}
