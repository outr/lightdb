package spec

import lightdb.mariadb.MariaDBStoreManager

@EmbeddedTest
class MariaDBSpec extends AbstractBasicSpec with MariaDBAvailability {
  override lazy val storeManager: MariaDBStoreManager = MariaDBTestSupport.storeManager

  override protected def filterBuilderSupported: Boolean = true
  // Rows that tie on score come back in no fixed order, as on Lucene.
  override protected def scoredResultsOrderingSupported: Boolean = false
}
