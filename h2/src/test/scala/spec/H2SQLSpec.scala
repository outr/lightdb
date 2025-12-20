package spec

import lightdb.h2.H2Store
import lightdb.sql.SQLCollectionManager

@EmbeddedTest
class H2SQLSpec extends AbstractSQLSpec {
  override def storeManager: SQLCollectionManager = H2Store
}
