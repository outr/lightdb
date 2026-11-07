package spec

import lightdb.mariadb.MariaDBStoreManager

@EmbeddedTest
class MariaDBBoostedBestMatchSpec extends AbstractBoostedBestMatchSpec with MariaDBAvailability {
  override lazy val storeManager: MariaDBStoreManager = MariaDBTestSupport.storeManager
}
