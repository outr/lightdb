package spec

import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneExistsChildSpec extends AbstractExistsChildSpec {
  override def storeManager: LuceneStore.type = LuceneStore
}

