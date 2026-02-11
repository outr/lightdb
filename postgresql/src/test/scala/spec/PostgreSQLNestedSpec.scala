package spec

import lightdb.postgresql.PostgreSQLStoreManager
import lightdb.sql.connect.{HikariConnectionManager, SQLConfig}
import lightdb.store.CollectionManager

@EmbeddedTest
class PostgreSQLNestedSpec extends AbstractNestedSpec {
  override lazy val storeManager: CollectionManager = PostgreSQLStoreManager(HikariConnectionManager(SQLConfig(
    jdbcUrl = s"jdbc:postgresql://localhost:5432/basic",
    username = Some("postgres"),
    password = Some("password")
  )))
}
