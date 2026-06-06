package spec

import lightdb.postgresql.PostgreSQLStoreManager

@EmbeddedTest
class PostgreSQLSpec extends AbstractBasicSpec with PostgreSQLAvailability {
  override lazy val storeManager: PostgreSQLStoreManager = PostgreSQLTestSupport.storeManager
}
