package spec
import lightdb.sql.SQLiteStore
import lightdb.store.StoreManager

class SQLiteSpecialCasesSpec extends AbstractSpecialCasesSpec {
  override def storeManager: StoreManager = SQLiteStore
}
