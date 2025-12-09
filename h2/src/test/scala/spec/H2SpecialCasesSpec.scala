package spec

import lightdb.h2.H2Store

@EmbeddedTest
class H2SpecialCasesSpec extends AbstractSpecialCasesSpec {
  override def storeManager: H2Store.type = H2Store
}
