package spec

import lightdb.store.CollectionManager

@EmbeddedTest
class PostgreSQLRetiredColumnSpec extends AbstractRetiredColumnSpec with PostgreSQLAvailability {
  override protected def storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
  override protected def embedded: Boolean = false
}
