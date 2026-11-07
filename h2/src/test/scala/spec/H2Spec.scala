package spec
import lightdb.h2.H2Store
import lightdb.store.CollectionManager

@EmbeddedTest
class H2Spec extends AbstractBasicSpec {
  override def storeManager: CollectionManager = H2Store

  override protected def filterBuilderSupported: Boolean = true
  // Rows that tie on score come back in no fixed order, as on Lucene.
  override protected def scoredResultsOrderingSupported: Boolean = false
}
