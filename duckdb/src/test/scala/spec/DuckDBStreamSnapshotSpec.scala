package spec

import lightdb.duckdb.DuckDBStore

@EmbeddedTest
class DuckDBStreamSnapshotSpec extends AbstractStreamSnapshotSpec {
  override def storeManager: DuckDBStore.type = DuckDBStore
}
