package spec

import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager

@EmbeddedTest
class SQLiteRetiredColumnSpec extends AbstractRetiredColumnSpec {
  override protected def storeManager: CollectionManager = SQLiteStore
}
