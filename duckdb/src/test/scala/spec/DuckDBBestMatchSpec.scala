package spec

import lightdb.duckdb.DuckDBStore

@EmbeddedTest
class DuckDBBestMatchSpec extends AbstractBestMatchSpec {
  override def storeManager: DuckDBStore.type = DuckDBStore
}
