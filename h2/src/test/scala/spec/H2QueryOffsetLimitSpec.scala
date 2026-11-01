package spec

import lightdb.h2.H2Store

@EmbeddedTest
class H2QueryOffsetLimitSpec extends AbstractQueryOffsetLimitSpec {
  override def storeManager: H2Store.type = H2Store
}
