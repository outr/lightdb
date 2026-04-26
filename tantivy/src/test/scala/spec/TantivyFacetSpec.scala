package spec

import lightdb.store.CollectionManager
import lightdb.tantivy.TantivyStore

@EmbeddedTest
class TantivyFacetSpec extends AbstractFacetSpec {
  override def storeManager: CollectionManager = TantivyStore
}
