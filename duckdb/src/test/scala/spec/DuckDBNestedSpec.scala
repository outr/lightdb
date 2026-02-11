package spec

import lightdb.duckdb.DuckDBStore
import lightdb.store.CollectionManager

@EmbeddedTest
class DuckDBNestedSpec extends AbstractNestedSpec {
  override def storeManager: CollectionManager = DuckDBStore
}
