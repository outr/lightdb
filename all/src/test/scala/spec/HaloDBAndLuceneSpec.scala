package spec
import lightdb.halodb.HaloDBStore
import lightdb.lucene.LuceneStore
import lightdb.store.{StoreManager, StoreMode}
import lightdb.store.split.SplitStoreManager

class HaloDBAndLuceneSpec extends AbstractBasicSpec {
  override def storeManager: StoreManager = SplitStoreManager(HaloDBStore, LuceneStore) //, searchingMode = StoreMode.Indexes)
}
