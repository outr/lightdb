package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchNestedSpec extends AbstractNestedSpec with OpenSearchTestSupport {
  override def storeManager: OpenSearchStore.type = OpenSearchStore
}

