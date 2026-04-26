package spec

import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyBasicSpec extends AbstractBasicSpec {
  override def storeManager: TantivyStore.type = TantivyStore
}
