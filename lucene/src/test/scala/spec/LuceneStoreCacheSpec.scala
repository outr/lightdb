package spec

import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneStoreCacheSpec extends AbstractStoreCacheSpec {
  override protected def storeManager: LuceneStore.type = LuceneStore
}
