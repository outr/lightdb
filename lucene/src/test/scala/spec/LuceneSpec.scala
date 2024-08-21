package spec

import lightdb.lucene.LuceneStore
import lightdb.store.StoreManager

@EmbeddedTest
class LuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override def storeManager: StoreManager = LuceneStore
}
