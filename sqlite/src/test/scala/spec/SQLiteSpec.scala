package spec
import lightdb.sql.SQLiteStore

@EmbeddedTest
class SQLiteSpec extends AbstractBasicSpec {
//  SQLQueryBuilder.LogQueries = true
//  addFeature(SQLDatabase.Key, SQLDatabase(SQLiteStore.singleConnectionManager(Some(Path.of("db", specName)))))

  override def storeManager: SQLiteStore.type = SQLiteStore
}