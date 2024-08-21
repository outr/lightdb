package spec
import lightdb.duckdb.DuckDBStore
import lightdb.store.StoreManager

//@EmbeddedTest
class DuckDBSpec extends AbstractBasicSpec {
  override def storeManager: StoreManager = DuckDBStore
}
