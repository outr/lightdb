package spec

import lightdb.duckdb.DuckDBStore
import lightdb.store.CollectionManager

@EmbeddedTest
class DuckDBRetiredColumnSpec extends AbstractRetiredColumnSpec {
  override protected def storeManager: CollectionManager = DuckDBStore
}
