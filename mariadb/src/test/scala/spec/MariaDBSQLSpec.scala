package spec

import lightdb.sql.SQLCollectionManager

@EmbeddedTest
class MariaDBSQLSpec extends AbstractSQLSpec with MariaDBAvailability {
  override def storeManager: SQLCollectionManager = MariaDBTestSupport.storeManager
}
