package spec
import lightdb.sql.SQLiteStore
import lightdb.store.StoreManager

class SQLiteSpec extends AbstractBasicSpec {
  override def storeManager: StoreManager = SQLiteStore
}