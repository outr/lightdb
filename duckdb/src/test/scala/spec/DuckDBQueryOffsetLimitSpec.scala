package spec

import lightdb.duckdb.DuckDBStore

@EmbeddedTest
class DuckDBQueryOffsetLimitSpec extends AbstractQueryOffsetLimitSpec {
  override def storeManager: DuckDBStore.type = DuckDBStore
}
