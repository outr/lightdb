package spec

import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneQueryOffsetLimitSpec extends AbstractQueryOffsetLimitSpec {
  override def storeManager: LuceneStore.type = LuceneStore
}
