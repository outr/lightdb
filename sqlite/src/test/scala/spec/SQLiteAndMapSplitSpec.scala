package spec

import lightdb.sql.SQLiteStore
import lightdb.store.split.SplitStoreManager
import lightdb.store.{CollectionManager, MapStore}

@EmbeddedTest
class SQLiteAndMapSplitSpec extends AbstractBasicSpec {
  override protected def memoryOnly: Boolean = true

  override def storeManager: CollectionManager = SplitStoreManager(MapStore, SQLiteStore)
}