package spec

import lightdb.store.CollectionManager

@EmbeddedTest
class MariaDBNestedSpec extends AbstractNestedSpec with MariaDBAvailability {
  override lazy val storeManager: CollectionManager = MariaDBTestSupport.storeManager
}
