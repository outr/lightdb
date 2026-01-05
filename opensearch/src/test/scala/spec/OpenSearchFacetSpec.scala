package spec

import lightdb.opensearch.OpenSearchStore
import lightdb.store.CollectionManager

@EmbeddedTest
class OpenSearchFacetSpec extends AbstractFacetSpec with OpenSearchTestSupport {
  override def storeManager: CollectionManager = OpenSearchStore
}


