package spec

import lightdb.arangodb.ArangoDBStoreManager
import lightdb.store.CollectionManager

@EmbeddedTest
class ArangoDBSpec extends AbstractBasicSpec with ArangoDBAvailability {
  override def storeManager: CollectionManager =
    ArangoDBStoreManager(ArangoDBTestSupport.config.get, databaseName = Some(specName))
}
