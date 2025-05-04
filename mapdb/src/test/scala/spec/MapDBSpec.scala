package spec

import lightdb.mapdb.MapDBStore
import lightdb.store.StoreManager

@EmbeddedTest
class MapDBSpec extends AbstractKeyValueSpec {
  override def storeManager: StoreManager = MapDBStore
}
