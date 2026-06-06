package spec

import lightdb.store.CollectionManager

@EmbeddedTest
class PostgreSQLNestedSpec extends AbstractNestedSpec with PostgreSQLAvailability {
  override lazy val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
}
