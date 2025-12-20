package spec

import lightdb.duckdb.DuckDBStore
import lightdb.sql.SQLCollectionManager

@EmbeddedTest
class DuckDBSQLSpec extends AbstractSQLSpec {
  override def storeManager: SQLCollectionManager = DuckDBStore
}
