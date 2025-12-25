package spec

import lightdb.opensearch.OpenSearchStore
import lightdb.store.StoreManager

@EmbeddedTest
class OpenSearchKeyValueSpec extends AbstractKeyValueSpec with OpenSearchTestSupport {
  override def storeManager: StoreManager = OpenSearchStore
}



