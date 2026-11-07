package spec

import lightdb.postgresql.PostgreSQLStoreManager

@EmbeddedTest
class PostgreSQLBoostedBestMatchSpec extends AbstractBoostedBestMatchSpec with PostgreSQLAvailability {
  override lazy val storeManager: PostgreSQLStoreManager = PostgreSQLTestSupport.storeManager
}
