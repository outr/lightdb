package spec

import lightdb.duckdb.DuckDBStore

@EmbeddedTest
class DuckDBStreamInterleaveSpec extends AbstractStreamInterleaveSpec {
  override def storeManager: DuckDBStore.type = DuckDBStore
}
