package spec

import lightdb.store.CollectionManager

@EmbeddedTest
class MariaDBRetiredColumnSpec extends AbstractRetiredColumnSpec with MariaDBAvailability {
  override protected def storeManager: CollectionManager = MariaDBTestSupport.storeManager
  override protected def embedded: Boolean = false
}
