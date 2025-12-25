package spec

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.Duration

private[spec] object OpenSearchTestContainer {
  private val HttpPort = 9200

  private class Container(image: DockerImageName) extends GenericContainer[Container](image)

  private lazy val container: Container = {
    val image = sys.props.getOrElse("lightdb.opensearch.testcontainers.image", "opensearchproject/opensearch:2.13.0")
    val c = new Container(DockerImageName.parse(image))
    c.withExposedPorts(HttpPort)
    c.withEnv("discovery.type", "single-node")
    // Disable the security plugin for a simple unauthenticated HTTP endpoint in tests.
    c.withEnv("DISABLE_SECURITY_PLUGIN", "true")
    // Keep resource usage reasonable for local dev and CI.
    c.withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
    c.waitingFor(Wait.forHttp("/").forPort(HttpPort).forStatusCode(200).withStartupTimeout(Duration.ofMinutes(3)))
    c
  }

  private lazy val started: Unit = {
    container.start()
    sys.addShutdownHook {
      try {
        container.stop()
      } catch {
        case _: Throwable => // ignore
      }
    }
  }

  def baseUrl: String = {
    started
    val host = container.getHost
    val port = container.getMappedPort(HttpPort)
    s"http://$host:$port"
  }
}



