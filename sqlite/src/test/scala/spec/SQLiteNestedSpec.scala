package spec

import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager

@EmbeddedTest
class SQLiteNestedSpec extends AbstractNestedSpec {
  override def storeManager: CollectionManager = SQLiteStore
}

