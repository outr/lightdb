package spec

import com.mongodb.client.MongoClients
import lightdb.mongodb.MongoDBStoreManager
import lightdb.store.CollectionManager

@EmbeddedTest
class MongoDBSpec extends AbstractBasicSpec with MongoDBAvailability {
  // Like the SQL-family backends, MongoDB has no relevance scoring at this layer, so the
  // `Filter.Builder` scoring surface stays off (only Lucene/OpenSearch enable it). Boolean
  // composition itself (must/should/mustNot/minShould) is still supported via $and/$or/$nor.

  // A networked store has no local directory to wipe, and AbstractBasicSpec asserts an empty store at
  // start. Drop any stale data from a prior local-mongod run before the suite executes (no-op when
  // MongoDB is unavailable — the suite is then cancelled by MongoDBAvailability).
  MongoDBTestSupport.connectionString.foreach { cs =>
    val client = MongoClients.create(cs)
    try client.getDatabase(specName).drop()
    finally client.close()
  }

  override def storeManager: CollectionManager =
    MongoDBStoreManager(MongoDBTestSupport.connectionString.get, databaseName = Some(specName))
}
