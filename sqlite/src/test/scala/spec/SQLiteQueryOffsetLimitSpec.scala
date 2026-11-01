package spec

import lightdb.sql.SQLiteStore

@EmbeddedTest
class SQLiteQueryOffsetLimitSpec extends AbstractQueryOffsetLimitSpec {
  override def storeManager: SQLiteStore.type = SQLiteStore
}
