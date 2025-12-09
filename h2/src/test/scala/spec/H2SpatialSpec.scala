package spec

import lightdb.h2.H2Store
import lightdb.store.CollectionManager

@EmbeddedTest
class H2SpatialSpec extends AbstractSpatialSpec {
  override protected def storeManager: CollectionManager = H2Store
}