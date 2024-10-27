package spec

import lightdb.halodb.HaloDBStore
import lightdb.store.StoreManager

@EmbeddedTest
class HaloDBSpec extends AbstractKeyValueSpec {
  override def storeManager: StoreManager = HaloDBStore
}
