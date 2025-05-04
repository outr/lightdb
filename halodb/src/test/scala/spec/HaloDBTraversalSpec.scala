package spec

import lightdb.halodb.HaloDBStore
import lightdb.store.StoreManager

//@EmbeddedTest
class HaloDBTraversalSpec extends AbstractTraversalSpec {
  override def storeManager: StoreManager = HaloDBStore
}
