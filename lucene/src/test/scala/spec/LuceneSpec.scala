package spec

import lightdb.lucene.LuceneStore

@EmbeddedTest
class LuceneSpec extends AbstractBasicSpec {
  override protected def filterBuilderSupported: Boolean = true

  // Lucene's tie-break for equal-score docs is internal docId, which is implementation-defined
  // and shifts when the strict-insert NRT existence probe sees different segment compositions.
  override protected def scoredResultsOrderingSupported: Boolean = false

  override def storeManager: LuceneStore.type = LuceneStore
}
