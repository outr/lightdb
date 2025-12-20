package spec
import lightdb.sqlite.SQLiteStore
import lightdb.store.CollectionManager

@EmbeddedTest
class SQLiteFacetSpec extends AbstractFacetSpec {
  override def storeManager: CollectionManager = SQLiteStore
}
