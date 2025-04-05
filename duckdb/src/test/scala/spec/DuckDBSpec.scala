package spec
import lightdb.duckdb.DuckDBStore
import lightdb.store.CollectionManager

//@EmbeddedTest
class DuckDBSpec extends AbstractBasicSpec {
  override def storeManager: CollectionManager = DuckDBStore
}
