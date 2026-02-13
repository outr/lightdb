package spec

import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneNestedSpec extends AbstractNestedSpec {
  override def storeManager: LuceneStore.type = LuceneStore
}

