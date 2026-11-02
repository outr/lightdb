package spec

import lightdb.sql.SQLiteStore
import lightdb.store.StoreManager

@EmbeddedTest
class SQLiteConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = SQLiteStore
}
