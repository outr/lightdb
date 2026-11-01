package spec

import lightdb.mariadb.MariaDBStoreManager

@EmbeddedTest
class MariaDBBestMatchSpec extends AbstractBestMatchSpec with MariaDBAvailability {
  override def storeManager: MariaDBStoreManager = MariaDBTestSupport.storeManager
}
