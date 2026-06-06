package spec

import lightdb.postgresql.PostgreSQLStoreManager
import lightdb.sql.connect.{HikariConnectionManager, SQLConfig}

import java.net.{InetSocketAddress, Socket}

/**
 * Resolves a PostgreSQL connection for tests, preferring a locally-running instance and only
 * spinning up Docker when needed:
 *
 *   1. probe localhost:5432 — if reachable, use the local server (fast, nothing to start/stop);
 *   2. otherwise start an ephemeral Testcontainers PostgreSQL (auto-removed at JVM exit);
 *   3. if Docker is also unavailable, `None` — specs cancel gracefully (see [[PostgreSQLAvailability]]).
 */
object PostgreSQLTestSupport {
  val username = "postgres"
  val password = "password"

  lazy val jdbcUrl: Option[String] = localUrl().orElse(containerUrl())

  def sqlConfig: SQLConfig = SQLConfig(jdbcUrl = jdbcUrl.get, username = Some(username), password = Some(password))

  def storeManager: PostgreSQLStoreManager = PostgreSQLStoreManager(HikariConnectionManager(sqlConfig))

  private def localUrl(): Option[String] = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress("localhost", 5432), 500)
      Some("jdbc:postgresql://localhost:5432/basic")
    } catch {
      case _: Throwable => None
    } finally {
      try socket.close()
      catch { case _: Throwable => () }
    }
  }

  private def containerUrl(): Option[String] =
    try Some(PostgreSQLTestContainer.jdbcUrl)
    catch {
      case t: Throwable =>
        scribe.warn(s"PostgreSQL unavailable: no local instance on :5432 and Testcontainers failed to start (${t.getMessage})")
        None
    }
}
