package spec

import lightdb.chroniclemap.ChronicleMapStore
import lightdb.lucene.LuceneStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class ChronicleMapAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  // Tie-break ordering is implementation-defined under Lucene strict-insert NRT probing.
  override protected def scoredResultsOrderingSupported: Boolean = false

  override lazy val storeManager: SplitStoreManager[ChronicleMapStore.type, LuceneStore.type] = SplitStoreManager(ChronicleMapStore, LuceneStore)
}
