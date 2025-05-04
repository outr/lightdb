package spec

import lightdb.lucene.LuceneStore

//@EmbeddedTest
class LuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  override def storeManager: LuceneStore.type = LuceneStore
}
