package spec

import lightdb.sql.SQLiteStore

@EmbeddedTest
class SQLiteStreamInterleaveSpec extends AbstractStreamInterleaveSpec {
  override def storeManager: SQLiteStore.type = SQLiteStore
}
