package spec

import lightdb.sql.SQLiteStore

@EmbeddedTest
class SQLiteBoostedBestMatchSpec extends AbstractBoostedBestMatchSpec {
  override def storeManager: SQLiteStore.type = SQLiteStore
}
