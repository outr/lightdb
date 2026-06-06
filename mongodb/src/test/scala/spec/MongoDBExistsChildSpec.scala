package spec

import com.mongodb.client.MongoClients
import lightdb.mongodb.MongoDBStoreManager
import lightdb.store.CollectionManager

@EmbeddedTest
class MongoDBExistsChildSpec extends AbstractExistsChildSpec with MongoDBAvailability {
  private val dbName = getClass.getSimpleName
  MongoDBTestSupport.connectionString.foreach { cs =>
    val client = MongoClients.create(cs); try client.getDatabase(dbName).drop() finally client.close()
  }
  override def storeManager: CollectionManager =
    MongoDBStoreManager(MongoDBTestSupport.connectionString.get, databaseName = Some(dbName))
}
