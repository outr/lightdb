package spec
import lightdb.sql.SQLiteStore
import lightdb.store.StoreManager

@EmbeddedTest
class SQLiteSpecialCasesSpec extends AbstractSpecialCasesSpec {
  override def storeManager: StoreManager = SQLiteStore
}
