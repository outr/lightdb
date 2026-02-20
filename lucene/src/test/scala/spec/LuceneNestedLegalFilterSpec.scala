package spec

import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneNestedLegalFilterSpec extends AbstractNestedLegalFilterSpec {
  override def storeManager: LuceneStore.type = LuceneStore
}
