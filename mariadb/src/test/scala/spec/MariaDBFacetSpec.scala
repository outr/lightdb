package spec

import lightdb.mariadb.MariaDBStoreManager

@EmbeddedTest
class MariaDBFacetSpec extends AbstractFacetSpec with MariaDBAvailability {
  override lazy val storeManager: MariaDBStoreManager = MariaDBTestSupport.storeManager
}
