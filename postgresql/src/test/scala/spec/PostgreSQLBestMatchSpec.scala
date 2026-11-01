package spec

import lightdb.postgresql.PostgreSQLStoreManager

@EmbeddedTest
class PostgreSQLBestMatchSpec extends AbstractBestMatchSpec with PostgreSQLAvailability {
  override lazy val storeManager: PostgreSQLStoreManager = PostgreSQLTestSupport.storeManager
}
