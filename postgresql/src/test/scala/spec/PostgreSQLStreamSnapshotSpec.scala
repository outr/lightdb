package spec

import lightdb.postgresql.PostgreSQLStoreManager

@EmbeddedTest
class PostgreSQLStreamSnapshotSpec extends AbstractStreamSnapshotSpec with PostgreSQLAvailability {
  override lazy val storeManager: PostgreSQLStoreManager = PostgreSQLTestSupport.storeManager
}
