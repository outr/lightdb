package spec

import lightdb.sql.SQLiteStore
import lightdb.store.StoreManager

@EmbeddedTest
class SQLiteSpatialSpec extends AbstractSpatialSpec {
  override protected def storeManager: StoreManager = SQLiteStore
}