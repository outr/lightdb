package spec

import lightdb.sql.SQLiteStore
import lightdb.store.{MapStore, StoreManager}
import lightdb.store.split.SplitStoreManager

class SQLiteAndMapSplitSpec extends AbstractBasicSpec {
  override protected def memoryOnly: Boolean = true

  override def storeManager: StoreManager = SplitStoreManager(MapStore, SQLiteStore)
}