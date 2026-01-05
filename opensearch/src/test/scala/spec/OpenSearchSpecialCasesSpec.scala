package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchSpecialCasesSpec extends AbstractSpecialCasesSpec with OpenSearchTestSupport {
  override def storeManager: OpenSearchStore.type = OpenSearchStore
}



