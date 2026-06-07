package spec

import com.arangodb.ArangoDB
import lightdb.arangodb.ArangoDBStoreManager
import lightdb.store.CollectionManager

@EmbeddedTest
class ArangoDBNestedLegalFilterSpec extends AbstractNestedLegalFilterSpec with ArangoDBAvailability {
  private val dbName = getClass.getSimpleName
  // Clean any stale data from a prior run (drop the per-spec database).
  ArangoDBTestSupport.config.foreach { c =>
    val client = new ArangoDB.Builder().host(c.host, c.port).user(c.user).password(c.password).build()
    try { val db = client.db(dbName); if (db.exists()) db.drop() } finally client.shutdown()
  }
  override def storeManager: CollectionManager =
    ArangoDBStoreManager(ArangoDBTestSupport.config.get, databaseName = Some(dbName))
}
