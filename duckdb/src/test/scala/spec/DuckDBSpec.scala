package spec
import lightdb.duckdb.DuckDBStore
import lightdb.store.{CollectionManager, StoreManager}

//@EmbeddedTest
class DuckDBSpec extends AbstractBasicSpec {
  override def storeManager: CollectionManager = DuckDBStore
}
