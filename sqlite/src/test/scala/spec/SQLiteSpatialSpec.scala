package spec

import lightdb.sql.{SQLQueryBuilder, SQLiteStore}
import lightdb.store.StoreManager

@EmbeddedTest
class SQLiteSpatialSpec extends AbstractSpatialSpec {
  override protected def storeManager: StoreManager = SQLiteStore
}