package spec

import lightdb.duckdb.DuckDBStore
import lightdb.store.CollectionManager

@EmbeddedTest
class DuckDBSQLSpec extends AbstractSQLSpec {
  override def storeManager: CollectionManager = DuckDBStore
}
