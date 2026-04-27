package spec
import lightdb.halodb.HaloDBStore
import lightdb.lucene.LuceneStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class HaloDBAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  // Tie-break ordering is implementation-defined under Lucene strict-insert NRT probing.
  override protected def scoredResultsOrderingSupported: Boolean = false

  override def storeManager: SplitStoreManager[HaloDBStore.type, LuceneStore.type] = SplitStoreManager(HaloDBStore, LuceneStore)
}