package spec

import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyExistsChildSpec extends AbstractExistsChildSpec {
  override def storeManager: TantivyStore.type = TantivyStore
}
