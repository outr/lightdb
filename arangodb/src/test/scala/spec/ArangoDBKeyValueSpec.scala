package spec

import lightdb.arangodb.ArangoDBStoreManager
import lightdb.store.StoreManager

@EmbeddedTest
class ArangoDBKeyValueSpec extends AbstractKeyValueSpec with ArangoDBAvailability {
  // Networked store: no local directory to wipe, so explicitly truncate both stores before inserting.
  override def truncateAfter: Boolean = false

  override def storeManager: StoreManager =
    ArangoDBStoreManager(ArangoDBTestSupport.config.get, databaseName = Some(specName))
}
