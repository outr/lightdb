package spec

import com.mongodb.client.MongoClients
import lightdb.mongodb.MongoDBStoreManager
import lightdb.store.CollectionManager

@EmbeddedTest
class MongoDBSpatialSpec extends AbstractSpatialSpec with MongoDBAvailability {
  private val dbName = getClass.getSimpleName
  MongoDBTestSupport.connectionString.foreach { cs =>
    val client = MongoClients.create(cs); try client.getDatabase(dbName).drop() finally client.close()
  }
  override protected def storeManager: CollectionManager =
    MongoDBStoreManager(MongoDBTestSupport.connectionString.get, databaseName = Some(dbName))
}
