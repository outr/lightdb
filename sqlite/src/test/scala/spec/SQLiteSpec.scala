package spec
import lightdb.sql.SQLiteStore

@EmbeddedTest
class SQLiteSpec extends AbstractBasicSpec {
//  SQLQueryBuilder.LogQueries = true
//  addFeature(SQLDatabase.Key, SQLDatabase(SQLiteStore.singleConnectionManager(Some(Path.of("db", specName)))))

  override def storeManager: SQLiteStore.type = SQLiteStore

  override protected def filterBuilderSupported: Boolean = true
  // Rows that tie on score come back in no fixed order, as on Lucene.
  override protected def scoredResultsOrderingSupported: Boolean = false
}
