package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchDeliveryPathSpec extends AbstractDeliveryPathSpec with OpenSearchTestSupport {
  override def storeManager = OpenSearchStore
}


