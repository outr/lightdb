package spec

import lightdb.mariadb.MariaDBStoreManager

@EmbeddedTest
class MariaDBStreamSnapshotSpec extends AbstractStreamSnapshotSpec with MariaDBAvailability {
  override def storeManager: MariaDBStoreManager = MariaDBTestSupport.storeManager
}
