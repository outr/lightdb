package spec
import lightdb.halodb.HaloDBStore
import lightdb.lucene.LuceneStore
import lightdb.store.{StoreManager, StoreMode}
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class HaloDBAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override def storeManager: StoreManager = SplitStoreManager(HaloDBStore, LuceneStore)
}