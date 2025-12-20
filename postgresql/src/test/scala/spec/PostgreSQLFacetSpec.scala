package spec

import lightdb.postgresql.PostgreSQLStoreManager
import lightdb.sql.connect.{HikariConnectionManager, SQLConfig}

@EmbeddedTest
class PostgreSQLFacetSpec extends AbstractFacetSpec {
  override lazy val storeManager: PostgreSQLStoreManager = PostgreSQLStoreManager(HikariConnectionManager(SQLConfig(
    jdbcUrl = s"jdbc:postgresql://localhost:5432/basic",
    username = Some("postgres"),
    password = Some("password")
  )))
}


