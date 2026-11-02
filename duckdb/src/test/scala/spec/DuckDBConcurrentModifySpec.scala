package spec

import lightdb.duckdb.DuckDBStore
import lightdb.store.StoreManager

@EmbeddedTest
class DuckDBConcurrentModifySpec extends AbstractConcurrentModifySpec {
  override def storeManager: StoreManager = DuckDBStore
}
