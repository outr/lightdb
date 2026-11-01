package spec

import lightdb.mariadb.MariaDBStoreManager

@EmbeddedTest
class MariaDBQueryOffsetLimitSpec extends AbstractQueryOffsetLimitSpec with MariaDBAvailability {
  override def storeManager: MariaDBStoreManager = MariaDBTestSupport.storeManager
}
