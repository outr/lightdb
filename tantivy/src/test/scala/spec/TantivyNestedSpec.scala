package spec

import lightdb.store.CollectionManager
import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyNestedSpec extends AbstractNestedSpec {
  override def storeManager: CollectionManager = TantivyStore
}
