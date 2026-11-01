package spec

import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneBestMatchSpec extends AbstractBestMatchSpec {
  override def storeManager: LuceneStore.type = LuceneStore
}
