package spec

import lightdb.halodb.HaloDBStore
import lightdb.store.StoreManager

//@EmbeddedTest
class HaloDBEmployeeInfluenceSpec extends AbstractEmployeeInfluenceSpec {
  override def storeManager: StoreManager = HaloDBStore
}
