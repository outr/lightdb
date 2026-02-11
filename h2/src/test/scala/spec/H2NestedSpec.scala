package spec

import lightdb.h2.H2Store
import lightdb.store.CollectionManager

@EmbeddedTest
class H2NestedSpec extends AbstractNestedSpec {
  override def storeManager: CollectionManager = H2Store
}
