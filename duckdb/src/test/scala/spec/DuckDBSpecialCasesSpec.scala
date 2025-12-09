package spec

import lightdb.duckdb.DuckDBStore

@EmbeddedTest
class DuckDBSpecialCasesSpec extends AbstractSpecialCasesSpec {
  override def storeManager: DuckDBStore.type = DuckDBStore
}
