package spec
import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager

@EmbeddedTest
class SQLiteFacetSpec extends AbstractFacetSpec {
  override def storeManager: CollectionManager = SQLiteStore
}


