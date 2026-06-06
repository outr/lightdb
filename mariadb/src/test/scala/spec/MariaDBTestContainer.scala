package spec

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import fabric.rw.stringRW
import profig.Profig

import java.time.Duration

/**
 * Lazily starts a MariaDB container via Testcontainers and tears it down at JVM exit (Ryuk also
 * removes it if the JVM is hard-killed). Testcontainers 2.x doesn't publish the dedicated MariaDB
 * module, so this uses a `GenericContainer`. Image overridable via the
 * `lightdb.mariadb.testcontainers.image` Profig key.
 */
private[spec] object MariaDBTestContainer {
  private val Port = 3306

  private class Container(image: DockerImageName) extends GenericContainer[Container](image)

  private lazy val container: Container = {
    val image = Profig("lightdb.mariadb.testcontainers.image").opt[String].getOrElse("mariadb:11")
    val c = new Container(DockerImageName.parse(image))
    c.withExposedPorts(Port)
    c.withEnv("MARIADB_ROOT_PASSWORD", "password")
    c.withEnv("MARIADB_DATABASE", "basic")
    // The entrypoint logs "ready for connections" once for its temporary init server and again for
    // the real server — wait for the second.
    c.waitingFor(Wait.forLogMessage(".*ready for connections.*", 2).withStartupTimeout(Duration.ofMinutes(3)))
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
    s"jdbc:mariadb://${container.getHost}:${container.getMappedPort(Port)}/basic"
  }
}
