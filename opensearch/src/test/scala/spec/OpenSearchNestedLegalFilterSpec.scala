package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchNestedLegalFilterSpec extends AbstractNestedLegalFilterSpec with OpenSearchTestSupport {
  override def storeManager: OpenSearchStore.type = OpenSearchStore
}
