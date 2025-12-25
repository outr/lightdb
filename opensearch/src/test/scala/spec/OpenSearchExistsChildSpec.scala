package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchExistsChildSpec extends AbstractExistsChildSpec with OpenSearchTestSupport {
  override def storeManager: OpenSearchStore.type = OpenSearchStore
}


