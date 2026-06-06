package spec

import lightdb.mongodb.MongoDBStoreManager
import lightdb.store.StoreManager

@EmbeddedTest
class MongoDBKeyValueSpec extends AbstractKeyValueSpec with MongoDBAvailability {
  // Networked store: no local directory to wipe, so explicitly truncate both stores before inserting.
  override def truncateAfter: Boolean = false

  // A per-spec database name isolates this suite from any other MongoDB-backed spec.
  override def storeManager: StoreManager =
    MongoDBStoreManager(MongoDBTestSupport.connectionString.get, databaseName = Some(specName))
}
