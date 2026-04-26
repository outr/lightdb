package spec

import lightdb.store.CollectionManager
import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyNestedLegalFilterSpec extends AbstractNestedLegalFilterSpec {
  override def storeManager: CollectionManager = TantivyStore
}
