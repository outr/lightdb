package spec
import lightdb.sql.{SQLDatabase, SQLiteStore}
import lightdb.store.StoreManager

import java.nio.file.Path

@EmbeddedTest
class SQLiteSpec extends AbstractBasicSpec {
//  addFeature(SQLDatabase.Key, SQLDatabase(SQLiteStore.singleConnectionManager(Some(Path.of("db", specName)))))

  override def storeManager: StoreManager = SQLiteStore
}