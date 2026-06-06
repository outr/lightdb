package spec

import lightdb.mariadb.MariaDBStoreManager

@EmbeddedTest
class MariaDBSpecialCasesSpec extends AbstractSpecialCasesSpec with MariaDBAvailability {
  override def storeManager: MariaDBStoreManager = MariaDBTestSupport.storeManager
}
