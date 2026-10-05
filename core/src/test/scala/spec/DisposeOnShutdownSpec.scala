package spec

import lightdb.LightDB
import lightdb.store.hashmap.HashMapStore
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task

import java.io.File
import java.net.URLClassLoader
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

/**
 * Runs a database in a separate JVM that exits normally after initialization, and checks whether the JVM shutdown
 * hook disposed it: by default it does, and with `disposeOnShutdown = false` no hook is registered and nothing
 * disposes it.
 */
@EmbeddedTest
class DisposeOnShutdownSpec extends AnyWordSpec with Matchers {
  private val root = Files.createTempDirectory("DisposeOnShutdownSpec")

  private lazy val classpath: String = {
    val loaders = Iterator.iterate(getClass.getClassLoader)(_.getParent).takeWhile(_ != null).toList
    val urls = loaders.flatMap {
      case u: URLClassLoader => u.getURLs.toList.map(url => Path.of(url.toURI).toString)
      case _ => Nil
    }
    val own = Path.of(classOf[ShutdownHookDB].getProtectionDomain.getCodeSource.getLocation.toURI).toString
    (own :: urls ++ System.getProperty("java.class.path").split(File.pathSeparator).toList).distinct.mkString(File.pathSeparator)
  }

  /** Runs the child JVM to completion and returns its output and whether the database was disposed on exit. */
  private def runChild(disposeOnShutdown: Boolean): (String, Boolean) = {
    val marker = root.resolve(s"disposed-$disposeOnShutdown")
    val javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString
    val process = new ProcessBuilder(javaBin, "-Xmx256m", "-cp", classpath,
      "spec.ShutdownHookChild", marker.toString, disposeOnShutdown.toString)
      .redirectErrorStream(true)
      .start()
    val output = new String(process.getInputStream.readAllBytes())
    process.waitFor(60, TimeUnit.SECONDS) shouldBe true
    withClue(output)(process.exitValue() shouldBe 0)
    (output, Files.exists(marker))
  }

  "A database's JVM shutdown hook" should {
    "dispose the database on exit by default" in {
      val (output, disposed) = runChild(disposeOnShutdown = true)
      output should include("HOOK REGISTERED true")
      withClue(output)(disposed shouldBe true)
    }
    "not be registered when disposeOnShutdown is false" in {
      val (output, disposed) = runChild(disposeOnShutdown = false)
      output should include("HOOK REGISTERED false")
      withClue(output)(disposed shouldBe false)
    }
  }
}

/** An in-memory database that records its disposal in a marker file. */
class ShutdownHookDB(marker: Path, hook: Boolean) extends LightDB {
  override type SM = HashMapStore.type
  override val storeManager: SM = HashMapStore
  override def directory: Option[Path] = None
  override def upgrades: List[DatabaseUpgrade] = Nil
  override protected def disposeOnShutdown: Boolean = hook

  override protected def doDispose(): Task[Unit] = super.doDispose().map(_ => Files.writeString(marker, "disposed"))
}

object ShutdownHookChild {
  def main(args: Array[String]): Unit = {
    val db = new ShutdownHookDB(Path.of(args(0)), args(1).toBoolean)
    db.init.sync()
    println(s"HOOK REGISTERED ${db.shutdownHookRegistered}")
    System.out.flush()
    System.exit(0)
  }
}
