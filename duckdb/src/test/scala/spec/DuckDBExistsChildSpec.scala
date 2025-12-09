package spec

import lightdb.duckdb.DuckDBStore
import lightdb.store.CollectionManager

@EmbeddedTest
class DuckDBExistsChildSpec extends AbstractExistsChildSpec {
  override def storeManager: CollectionManager = DuckDBStore
}
