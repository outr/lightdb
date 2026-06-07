package spec

import fabric.rw.stringRW
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import profig.Profig

import java.time.Duration

/**
 * Lazily starts a Qdrant container via Testcontainers and tears it down at JVM exit. Exposes the gRPC
 * port (6334) used by the Java client. Image overridable via the `lightdb.qdrant.testcontainers.image`
 * Profig key.
 */
private[spec] object QdrantTestContainer {
  private val GrpcPort = 6334
  private val HttpPort = 6333

  private class Container(image: DockerImageName) extends GenericContainer[Container](image)

  private lazy val container: Container = {
    val image = Profig("lightdb.qdrant.testcontainers.image").opt[String].getOrElse("qdrant/qdrant:latest")
    val c = new Container(DockerImageName.parse(image))
    c.withExposedPorts(GrpcPort, HttpPort)
    c.waitingFor(Wait.forLogMessage(".*gRPC listening.*", 1).withStartupTimeout(Duration.ofMinutes(2)))
    c
  }

  private lazy val started: Unit = {
    container.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      try container.stop()
      catch { case _: Throwable => () }
    }))
  }

  def host: String = {
    started
    container.getHost
  }

  def grpcPort: Int = {
    started
    container.getMappedPort(GrpcPort)
  }
}
