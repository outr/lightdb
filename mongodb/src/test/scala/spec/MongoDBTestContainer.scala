package spec

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import fabric.rw.stringRW
import profig.Profig

import java.time.Duration

/**
 * Lazily starts a standalone MongoDB via Testcontainers and tears it down at JVM exit. Testcontainers'
 * Ryuk reaper also removes the container if the JVM is hard-killed, so nothing is left running.
 *
 * Testcontainers 2.x does not publish the dedicated `MongoDBContainer` module, so this uses a
 * `GenericContainer` (mirroring `OpenSearchTestContainer`). That yields a standalone mongod — fine
 * for the Phase 1 KV contract, which uses direct/buffered writes rather than Mongo sessions. Native
 * multi-document transactions would later require a replica set (`--replSet` + initiate).
 *
 * The image is overridable via the `lightdb.mongodb.testcontainers.image` Profig key.
 */
private[spec] object MongoDBTestContainer {
  private val MongoPort = 27017

  private class Container(image: DockerImageName) extends GenericContainer[Container](image)

  private lazy val container: Container = {
    val image = Profig("lightdb.mongodb.testcontainers.image").opt[String].getOrElse("mongo:7.0")
    val c = new Container(DockerImageName.parse(image))
    c.withExposedPorts(MongoPort)
    c.waitingFor(Wait.forLogMessage(".*Waiting for connections.*", 1).withStartupTimeout(Duration.ofMinutes(2)))
    c
  }

  private lazy val started: Unit = {
    container.start()
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      try container.stop()
      catch { case _: Throwable => () }
    }))
  }

  def connectionString: String = {
    started
    s"mongodb://${container.getHost}:${container.getMappedPort(MongoPort)}"
  }
}
