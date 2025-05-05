package spec

import lightdb.chroniclemap.ChronicleMapStore
import lightdb.lucene.LuceneStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class ChronicleMapAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override lazy val storeManager: SplitStoreManager[ChronicleMapStore.type, LuceneStore.type] = SplitStoreManager(ChronicleMapStore, LuceneStore)
}
