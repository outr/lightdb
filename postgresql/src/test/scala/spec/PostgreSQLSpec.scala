package spec

import lightdb.postgresql.PostgreSQLStoreManager

@EmbeddedTest
class PostgreSQLSpec extends AbstractBasicSpec with PostgreSQLAvailability {
  override lazy val storeManager: PostgreSQLStoreManager = PostgreSQLTestSupport.storeManager

  override protected def filterBuilderSupported: Boolean = true
  // Rows that tie on score come back in no fixed order, as on Lucene.
  override protected def scoredResultsOrderingSupported: Boolean = false
}
