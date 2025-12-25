package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchSpatialSpec extends AbstractSpatialSpec with OpenSearchTestSupport {
  override protected def storeManager: OpenSearchStore.type = OpenSearchStore
}


