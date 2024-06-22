package spec

import lightdb.mapdb.MapDBStore
import lightdb.store.StoreManager

class MapDBStoreSpec extends AbstractStoreSpec {
  override protected def storeManager: StoreManager = MapDBStore
}
