package spec

import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneNestedInNestedSpec extends AbstractNestedInNestedSpec {
  override def storeManager: LuceneStore.type = LuceneStore
}
