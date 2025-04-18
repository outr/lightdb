package spec

import lightdb.chroniclemap.ChronicleMapStore
import lightdb.lucene.LuceneStore
import lightdb.store.split.SplitStoreManager

@EmbeddedTest
class ChronicleMapAndLuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override lazy val storeManager: SplitStoreManager = SplitStoreManager(ChronicleMapStore, LuceneStore)
}
