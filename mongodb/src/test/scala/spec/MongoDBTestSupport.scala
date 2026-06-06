package spec

import java.net.{InetSocketAddress, Socket}

/**
 * Resolves a MongoDB connection string for tests, preferring a locally-running instance and only
 * spinning up Docker when needed:
 *
 *   1. probe localhost:27017 — if reachable, use the local mongod (fast, nothing to start/stop);
 *   2. otherwise start an ephemeral Testcontainers MongoDB (auto-removed at JVM exit);
 *   3. if Docker is also unavailable, `None` — specs cancel gracefully (see [[MongoDBAvailability]]).
 */
object MongoDBTestSupport {
  lazy val connectionString: Option[String] = localIfRunning().orElse(containerUri())

  private def localIfRunning(): Option[String] = {
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress("localhost", 27017), 500)
      Some("mongodb://localhost:27017")
    } catch {
      case _: Throwable => None
    } finally {
      try socket.close()
      catch { case _: Throwable => () }
    }
  }

  private def containerUri(): Option[String] =
    try Some(MongoDBTestContainer.connectionString)
    catch {
      case t: Throwable =>
        scribe.warn(s"MongoDB unavailable: no local instance on :27017 and Testcontainers failed to start (${t.getMessage})")
        None
    }
}
