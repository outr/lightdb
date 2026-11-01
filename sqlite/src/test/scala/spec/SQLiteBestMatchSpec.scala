package spec

import lightdb.sql.SQLiteStore

@EmbeddedTest
class SQLiteBestMatchSpec extends AbstractBestMatchSpec {
  override def storeManager: SQLiteStore.type = SQLiteStore
}
