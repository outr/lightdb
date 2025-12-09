package spec

import lightdb.duckdb.DuckDBStore
import lightdb.store.CollectionManager
import lightdb.store.hashmap.HashMapStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class DuckDBAndMapSplitSpec extends AbstractBasicSpec {
  override protected def memoryOnly: Boolean = true

  override def storeManager: CollectionManager = SplitStoreManager(HashMapStore, DuckDBStore)
}