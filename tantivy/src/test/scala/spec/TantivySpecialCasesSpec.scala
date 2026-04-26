package spec

import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivySpecialCasesSpec extends AbstractSpecialCasesSpec {
  override def storeManager: TantivyStore.type = TantivyStore
}
