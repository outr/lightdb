package spec

import lightdb.sql.SQLiteStore
import lightdb.store.split.SplitStoreManager
import lightdb.store.{MapStore, StoreManager}

@EmbeddedTest
class SQLiteAndMapSplitSpec extends AbstractBasicSpec {
  override protected def memoryOnly: Boolean = true

  override def storeManager: StoreManager = SplitStoreManager(MapStore, SQLiteStore)
}