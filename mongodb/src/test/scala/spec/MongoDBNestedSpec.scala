package spec

import com.mongodb.client.MongoClients
import lightdb.mongodb.MongoDBStoreManager
import lightdb.store.CollectionManager

@EmbeddedTest
class MongoDBNestedSpec extends AbstractNestedSpec with MongoDBAvailability {
  // Clean any stale data from a prior local-mongod run (networked store, no directory to wipe).
  MongoDBTestSupport.connectionString.foreach { cs =>
    val client = MongoClients.create(cs)
    try client.getDatabase(specName).drop()
    finally client.close()
  }

  override def storeManager: CollectionManager =
    MongoDBStoreManager(MongoDBTestSupport.connectionString.get, databaseName = Some(specName))
}
