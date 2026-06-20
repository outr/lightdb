package spec

import lightdb.store.CollectionManager

@EmbeddedTest
class PostgreSQLSpatialSpec extends AbstractSpatialSpec with PostgreSQLAvailability {
  override protected def storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
}
