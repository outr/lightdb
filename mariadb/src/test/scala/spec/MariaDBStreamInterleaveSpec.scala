package spec

import lightdb.mariadb.MariaDBStoreManager

@EmbeddedTest
class MariaDBStreamInterleaveSpec extends AbstractStreamInterleaveSpec with MariaDBAvailability {
  override def storeManager: MariaDBStoreManager = MariaDBTestSupport.storeManager
}
