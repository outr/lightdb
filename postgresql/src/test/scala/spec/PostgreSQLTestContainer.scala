package spec

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import fabric.rw.stringRW
import profig.Profig

import java.time.Duration

/**
 * Lazily starts a PostgreSQL container via Testcontainers and tears it down at JVM exit (Ryuk also
 * removes it if the JVM is hard-killed). Testcontainers 2.x doesn't publish the dedicated
 * `PostgreSQLContainer` module, so this uses a `GenericContainer` (mirroring the other backends).
 * Image overridable via the `lightdb.postgresql.testcontainers.image` Profig key.
 */
private[spec] object PostgreSQLTestContainer {
  private val Port = 5432

  private class Container(image: DockerImageName) extends GenericContainer[Container](image)

  private lazy val container: Container = {
    // PostGIS-enabled image (a superset of the official `postgres` image, incl. the `pg_trgm`
    // contrib module) so the spatial spec's `CREATE EXTENSION postgis` succeeds. Plain `postgres`
    // images lack PostGIS, which made the spatial spec pass locally (developer's :5432 server has
    // it) but fail in CI where the ephemeral container is the only server. Vector tests use a
    // separate pgvector container, so they're unaffected. Overridable via Profig.
    val image = Profig("lightdb.postgresql.testcontainers.image").opt[String].getOrElse("postgis/postgis:17-3.5")
    val c = new Container(DockerImageName.parse(image))
    c.withExposedPorts(Port)
    c.withEnv("POSTGRES_DB", "basic")
    c.withEnv("POSTGRES_USER", "postgres")
    c.withEnv("POSTGRES_PASSWORD", "password")
    // postgres logs this line twice (init, then the real start) — wait for the second.
    c.waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2).withStartupTimeout(Duration.ofMinutes(2)))
    c
  }

  private lazy val started: Unit = {
    container.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      try container.stop()
      catch { case _: Throwable => () }
    }))
  }

  def jdbcUrl: String = {
    started
    s"jdbc:postgresql://${container.getHost}:${container.getMappedPort(Port)}/basic"
  }
}
