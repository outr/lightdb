package spec
import lightdb.duckdb.DuckDBStore
import lightdb.store.CollectionManager

@EmbeddedTest
class DuckDBFacetSpec extends AbstractFacetSpec {
  override def storeManager: CollectionManager = DuckDBStore
}
