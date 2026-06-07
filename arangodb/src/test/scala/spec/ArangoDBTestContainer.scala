package spec

import lightdb.arangodb.ArangoDBConfig
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import fabric.rw.stringRW
import profig.Profig

import java.time.Duration

/**
 * Lazily starts an ArangoDB container via Testcontainers and tears it down at JVM exit (Ryuk also
 * removes it if the JVM is hard-killed). Image overridable via the
 * `lightdb.arangodb.testcontainers.image` Profig key.
 */
private[spec] object ArangoDBTestContainer {
  private val Port = 8529
  private val Password = "password"

  private class Container(image: DockerImageName) extends GenericContainer[Container](image)

  private lazy val container: Container = {
    val image = Profig("lightdb.arangodb.testcontainers.image").opt[String].getOrElse("arangodb:3.12")
    val c = new Container(DockerImageName.parse(image))
    c.withExposedPorts(Port)
    c.withEnv("ARANGO_ROOT_PASSWORD", Password)
    c.waitingFor(Wait.forLogMessage(".*ready for business.*", 1).withStartupTimeout(Duration.ofMinutes(3)))
    c
  }

  private lazy val started: Unit = {
    container.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      try container.stop()
      catch { case _: Throwable => () }
    }))
  }

  def config: ArangoDBConfig = {
    started
    ArangoDBConfig(container.getHost, container.getMappedPort(Port), "root", Password)
  }
}
