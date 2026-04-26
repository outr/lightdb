package spec

import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyStoreCacheSpec extends AbstractStoreCacheSpec {
  override def storeManager: TantivyStore.type = TantivyStore
}
