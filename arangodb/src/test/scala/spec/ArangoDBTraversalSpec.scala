package spec

import lightdb.arangodb.ArangoDBStoreManager
import lightdb.store.prefix.PrefixScanningStoreManager

@EmbeddedTest
class ArangoDBTraversalSpec extends AbstractTraversalSpec with ArangoDBAvailability {
  override def storeManager: PrefixScanningStoreManager =
    ArangoDBStoreManager(ArangoDBTestSupport.config.get, databaseName = Some("ArangoDBTraversalSpec"))
}
