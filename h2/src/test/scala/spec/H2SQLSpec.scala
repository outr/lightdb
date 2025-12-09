package spec

import lightdb.h2.H2Store
import lightdb.store.CollectionManager

@EmbeddedTest
class H2SQLSpec extends AbstractSQLSpec {
  override def storeManager: CollectionManager = H2Store
}
