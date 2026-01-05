package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchEmployeeInfluenceSpec extends AbstractEmployeeInfluenceSpec with OpenSearchTestSupport {
  override def storeManager = OpenSearchStore
}


