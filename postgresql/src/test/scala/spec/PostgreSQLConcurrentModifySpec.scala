package spec

import lightdb.store.StoreManager

@EmbeddedTest
class PostgreSQLConcurrentModifySpec extends AbstractConcurrentModifySpec with PostgreSQLAvailability {
  override lazy val storeManager: StoreManager = PostgreSQLTestSupport.storeManager
  override protected def otherProcess: Option[StoreManager] = Some(PostgreSQLTestSupport.storeManager)
}
