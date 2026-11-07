package spec

import lightdb.duckdb.DuckDBStore

@EmbeddedTest
class DuckDBBoostedBestMatchSpec extends AbstractBoostedBestMatchSpec {
  override def storeManager: DuckDBStore.type = DuckDBStore
}
