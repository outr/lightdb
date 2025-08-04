package spec

import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager

@EmbeddedTest
class SQLiteSQLSpec extends AbstractSQLSpec {
  override def storeManager: CollectionManager = SQLiteStore
}
