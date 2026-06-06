package spec

import lightdb.mariadb.MariaDBStoreManager
import lightdb.sql.connect.{HikariConnectionManager, SQLConfig}

import java.net.{InetSocketAddress, Socket}

/**
 * Resolves a MariaDB/MySQL connection for tests, preferring a locally-running server and only
 * spinning up Docker when needed:
 *
 *   1. probe localhost:3306 — if reachable, use the local server (fast path);
 *   2. otherwise start an ephemeral Testcontainers MariaDB (auto-removed at JVM exit);
 *   3. if Docker is also unavailable, `None` — specs cancel gracefully (see [[MariaDBAvailability]]).
 */
object MariaDBTestSupport {
  val username = "root"
  val password = "password"

  lazy val jdbcUrl: Option[String] = localUrl().orElse(containerUrl())

  def sqlConfig: SQLConfig = SQLConfig(
    jdbcUrl = jdbcUrl.get,
    username = Some(username),
    password = Some(password),
    driverClassName = Some("org.mariadb.jdbc.Driver"),
    // LightDB double-quotes identifiers; enable ANSI_QUOTES while preserving the other defaults.
    connectionInitSql = Some("SET SESSION sql_mode=CONCAT(@@sql_mode, ',ANSI_QUOTES')")
  )

  def storeManager: MariaDBStoreManager = MariaDBStoreManager(HikariConnectionManager(sqlConfig))

  private def localUrl(): Option[String] = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress("localhost", 3306), 500)
      Some("jdbc:mariadb://localhost:3306/basic")
    } catch {
      case _: Throwable => None
    } finally {
      try socket.close()
      catch { case _: Throwable => () }
    }
  }

  private def containerUrl(): Option[String] =
    try Some(MariaDBTestContainer.jdbcUrl)
    catch {
      case t: Throwable =>
        scribe.warn(s"MariaDB unavailable: no local server on :3306 and Testcontainers failed to start (${t.getMessage})")
        None
    }
}
