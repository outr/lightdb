package spec

import com.arangodb.ArangoDB
import lightdb.arangodb.ArangoDBStoreManager
import lightdb.store.prefix.PrefixScanningStoreManager

@EmbeddedTest
class ArangoDBEmployeeInfluenceSpec extends AbstractEmployeeInfluenceSpec with ArangoDBAvailability {
  private val dbName = getClass.getSimpleName
  ArangoDBTestSupport.config.foreach { c =>
    val client = new ArangoDB.Builder().host(c.host, c.port).user(c.user).password(c.password).build()
    try { val db = client.db(dbName); if (db.exists()) db.drop() } finally client.shutdown()
  }
  override def storeManager: PrefixScanningStoreManager =
    ArangoDBStoreManager(ArangoDBTestSupport.config.get, databaseName = Some(dbName))
}
