package spec

import lightdb.h2.H2Store

@EmbeddedTest
class H2BestMatchSpec extends AbstractBestMatchSpec {
  override def storeManager: H2Store.type = H2Store
}
