package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchTraversalSpec extends AbstractTraversalSpec with OpenSearchTestSupport {
  override def storeManager = OpenSearchStore
}


