package spec

import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager

@EmbeddedTest
class SQLiteUndeclaredStringFieldSpec extends AbstractUndeclaredStringFieldSpec {
  override protected def storeManager: CollectionManager = SQLiteStore
}
