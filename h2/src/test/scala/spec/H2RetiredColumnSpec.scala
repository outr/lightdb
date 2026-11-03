package spec

import lightdb.h2.H2Store
import lightdb.store.CollectionManager

@EmbeddedTest
class H2RetiredColumnSpec extends AbstractRetiredColumnSpec {
  override protected def storeManager: CollectionManager = H2Store
}
