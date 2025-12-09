package spec

import lightdb.h2.H2Store
import lightdb.store.CollectionManager

@EmbeddedTest
class H2ExistsChildSpec extends AbstractExistsChildSpec {
  override def storeManager: CollectionManager = H2Store
}
