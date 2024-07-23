package spec

import lightdb.postgresql.PostgreSQLStoreManager
import lightdb.sql.connect.{HikariConnectionManager, SQLConfig}
import lightdb.store.StoreManager

class PostgreSQLSpec extends AbstractBasicSpec {
  override lazy val storeManager: StoreManager = PostgreSQLStoreManager(HikariConnectionManager(SQLConfig(
    jdbcUrl = s"jdbc:postgresql://localhost:5432/basic",
    username = Some("postgres"),
    password = Some("password")
  )), connectionShared = false)
}
