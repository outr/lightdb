package spec

import fabric.rw.*
import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchNestedInNestedSpec extends AbstractNestedInNestedSpec with OpenSearchTestSupport {
  override def storeManager: OpenSearchStore.type = OpenSearchStore
}
