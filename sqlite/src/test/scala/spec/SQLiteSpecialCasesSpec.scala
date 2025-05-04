package spec
import lightdb.sql.SQLiteStore

//@EmbeddedTest
class SQLiteSpecialCasesSpec extends AbstractSpecialCasesSpec {
  override def storeManager: SQLiteStore.type = SQLiteStore
}
