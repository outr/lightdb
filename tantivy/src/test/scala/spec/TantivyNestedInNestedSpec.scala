package spec

import lightdb.store.CollectionManager
import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyNestedInNestedSpec extends AbstractNestedInNestedSpec {
  override def storeManager: CollectionManager = TantivyStore
}
