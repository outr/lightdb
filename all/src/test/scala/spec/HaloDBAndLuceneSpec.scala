package spec
import lightdb.halodb.HaloDBStore
import lightdb.lucene.LuceneStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class HaloDBAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override def storeManager: SplitStoreManager[HaloDBStore.type, LuceneStore.type] = SplitStoreManager(HaloDBStore, LuceneStore)
}