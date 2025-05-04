package spec

import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager

@EmbeddedTest
class SQLiteSpatialSpec extends AbstractSpatialSpec {
  override protected def storeManager: CollectionManager = SQLiteStore
}