package spec
import lightdb.duckdb.DuckDBStore
import lightdb.store.CollectionManager

@EmbeddedTest
class DuckDBSpec extends AbstractBasicSpec {
  override def storeManager: CollectionManager = DuckDBStore

  override protected def filterBuilderSupported: Boolean = true
  // Rows that tie on score come back in no fixed order, as on Lucene.
  override protected def scoredResultsOrderingSupported: Boolean = false
}
