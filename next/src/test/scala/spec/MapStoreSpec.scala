package spec

import lightdb.store.{MapStore, StoreManager}

class MapStoreSpec extends AbstractStoreSpec {
  override protected def storeManager: StoreManager = MapStore
}
