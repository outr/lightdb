package spec

import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyQueryOffsetLimitSpec extends AbstractQueryOffsetLimitSpec {
  override def storeManager: TantivyStore.type = TantivyStore
}
