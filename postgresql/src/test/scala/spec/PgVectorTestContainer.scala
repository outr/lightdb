package spec

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import fabric.rw.stringRW
import profig.Profig

import java.time.Duration

/**
 * Lazily starts a pgvector-enabled PostgreSQL container (the official `pgvector/pgvector` image, a
 * drop-in PostgreSQL with the `vector` extension available) for vector/KNN specs, and tears it down
 * at JVM exit. Unlike [[PostgreSQLTestContainer]] this does not probe for a local server, since a
 * plain local PostgreSQL would lack the extension. Image overridable via the
 * `lightdb.pgvector.testcontainers.image` Profig key.
 */
private[spec] object PgVectorTestContainer {
  private val Port = 5432

  private class Container(image: DockerImageName) extends GenericContainer[Container](image)

  private lazy val container: Container = {
    val image = Profig("lightdb.pgvector.testcontainers.image").opt[String].getOrElse("pgvector/pgvector:pg17")
    val c = new Container(DockerImageName.parse(image).asCompatibleSubstituteFor("postgres"))
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
