package spec

import com.mongodb.client.MongoClients
import lightdb.mongodb.MongoDBStoreManager
import lightdb.store.CollectionManager

@EmbeddedTest
class MongoDBNestedLegalFilterSpec extends AbstractNestedLegalFilterSpec with MongoDBAvailability {
  MongoDBTestSupport.connectionString.foreach { cs =>
    val client = MongoClients.create(cs)
    try client.getDatabase(specName).drop() finally client.close()
  }
  override def storeManager: CollectionManager =
    MongoDBStoreManager(MongoDBTestSupport.connectionString.get, databaseName = Some(specName))
}
