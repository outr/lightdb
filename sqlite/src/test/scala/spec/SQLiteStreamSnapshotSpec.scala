package spec

import lightdb.sql.SQLiteStore

@EmbeddedTest
class SQLiteStreamSnapshotSpec extends AbstractStreamSnapshotSpec {
  override def storeManager: SQLiteStore.type = SQLiteStore
}
