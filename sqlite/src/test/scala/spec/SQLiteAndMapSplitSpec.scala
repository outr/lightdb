package spec

import lightdb.sql.SQLiteStore
import lightdb.store.split.SplitStoreManager
import lightdb.store.CollectionManager
import lightdb.store.hashmap.HashMapStore

@EmbeddedTest
class SQLiteAndMapSplitSpec extends AbstractBasicSpec {
  override protected def memoryOnly: Boolean = true

  override def storeManager: CollectionManager = SplitStoreManager(HashMapStore, SQLiteStore)
}