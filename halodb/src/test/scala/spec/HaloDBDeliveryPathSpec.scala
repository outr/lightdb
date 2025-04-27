package spec

import lightdb.halodb.HaloDBStore
import lightdb.store.StoreManager

@EmbeddedTest
class HaloDBDeliveryPathSpec extends AbstractDeliveryPathSpec {
  override def storeManager: StoreManager = HaloDBStore
}
