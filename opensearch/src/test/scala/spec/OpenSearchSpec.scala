package spec

import lightdb.opensearch.OpenSearchStore

@EmbeddedTest
class OpenSearchSpec extends AbstractBasicSpec with OpenSearchTestSupport {
  // Keep enabled to drive parity with Lucene; we may temporarily disable while scoring is tuned.
  override protected def filterBuilderSupported: Boolean = true
  override protected def aggregationSupported: Boolean = true
  override protected def scoredResultsOrderingSupported: Boolean = false

  override def storeManager: OpenSearchStore.type = OpenSearchStore
}


