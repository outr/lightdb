package spec

import lightdb.store.CollectionManager

@EmbeddedTest
class PostgreSQLUndeclaredStringFieldSpec extends AbstractUndeclaredStringFieldSpec with PostgreSQLAvailability {
  override protected def storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
}
