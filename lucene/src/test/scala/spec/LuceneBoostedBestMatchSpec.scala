package spec

import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneBoostedBestMatchSpec extends AbstractBoostedBestMatchSpec {
  override def storeManager: LuceneStore.type = LuceneStore
}
