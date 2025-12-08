package spec

import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager

@EmbeddedTest
class SQLiteExistsChildSpec extends AbstractExistsChildSpec {
  override def storeManager: CollectionManager = SQLiteStore
}
