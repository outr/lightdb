package spec
import lightdb.sql.{SQLDatabase, SQLQueryBuilder, SQLiteStore}
import lightdb.store.StoreManager

import java.nio.file.Path

@EmbeddedTest
class SQLiteSpec extends AbstractBasicSpec {
//  SQLQueryBuilder.LogQueries = true
//  addFeature(SQLDatabase.Key, SQLDatabase(SQLiteStore.singleConnectionManager(Some(Path.of("db", specName)))))

  override def storeManager: StoreManager = SQLiteStore
}