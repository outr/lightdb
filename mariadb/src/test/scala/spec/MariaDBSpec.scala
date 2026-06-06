package spec

import lightdb.mariadb.MariaDBStoreManager

@EmbeddedTest
class MariaDBSpec extends AbstractBasicSpec with MariaDBAvailability {
  override lazy val storeManager: MariaDBStoreManager = MariaDBTestSupport.storeManager
}
