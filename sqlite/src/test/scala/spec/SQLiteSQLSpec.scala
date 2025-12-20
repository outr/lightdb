package spec

import lightdb.sql.SQLiteStore
import lightdb.sql.SQLCollectionManager

@EmbeddedTest
class SQLiteSQLSpec extends AbstractSQLSpec {
  override def storeManager: SQLCollectionManager = SQLiteStore
}
