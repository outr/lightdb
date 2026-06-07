package spec

import lightdb.postgresql.PostgreSQLStoreManager
import lightdb.sql.connect.{HikariConnectionManager, SQLConfig}

/**
 * Resolves a pgvector-enabled PostgreSQL for KNN specs via an ephemeral Testcontainers instance
 * (auto-removed at JVM exit). Yields `None` when Docker is unavailable so specs cancel gracefully
 * (see [[PgVectorAvailability]]).
 */
object PgVectorTestSupport {
  val username = "postgres"
  val password = "password"

  lazy val jdbcUrl: Option[String] =
    try Some(PgVectorTestContainer.jdbcUrl)
    catch {
      case t: Throwable =>
        scribe.warn(s"pgvector unavailable: Testcontainers failed to start (${t.getMessage})")
        None
    }

  def sqlConfig: SQLConfig = SQLConfig(jdbcUrl = jdbcUrl.get, username = Some(username), password = Some(password))

  def storeManager: PostgreSQLStoreManager = PostgreSQLStoreManager(HikariConnectionManager(sqlConfig))
}
