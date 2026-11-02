package spec

import lightdb.store.StoreManager

@EmbeddedTest
class MariaDBConcurrentModifySpec extends AbstractConcurrentModifySpec with MariaDBAvailability {
  override lazy val storeManager: StoreManager = MariaDBTestSupport.storeManager
  override protected def otherProcess: Option[StoreManager] = Some(MariaDBTestSupport.storeManager)
}
