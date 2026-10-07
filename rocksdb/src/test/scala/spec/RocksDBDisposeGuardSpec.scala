package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.StoreDisposedException
import lightdb.id.Id
import lightdb.rocksdb.{RocksDBSharedStore, RocksDBStore}
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.*

import java.io.File
import java.net.URLClassLoader
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.*

/**
 * Uses a RocksDB database after (and while) it is disposed, in a separate JVM so a native use-after-free cannot take
 * down the test runner: each use must be refused with a `StoreDisposedException` and the JVM must exit normally.
 */
@EmbeddedTest
class RocksDBDisposeGuardSpec extends AnyWordSpec with Matchers {
  private val root = Files.createTempDirectory("RocksDBDisposeGuardSpec")

  private lazy val classpath: String = {
    val loaders = Iterator.iterate(getClass.getClassLoader)(_.getParent).takeWhile(_ != null).toList
    val urls = loaders.flatMap {
      case u: URLClassLoader => u.getURLs.toList.map(url => Path.of(url.toURI).toString)
      case _ => Nil
    }
    val own = Path.of(classOf[DisposeGuardDB].getProtectionDomain.getCodeSource.getLocation.toURI).toString
    (own :: urls ++ System.getProperty("java.class.path").split(File.pathSeparator).toList).distinct.mkString(File.pathSeparator)
  }

  private def runChild(manager: String, op: String): String = {
    val dir = root.resolve(s"$manager-$op")
    val javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString
    val process = new ProcessBuilder(javaBin, "-Xmx512m", "-XX:ErrorFile=" + root.resolve("hs_err_%p.log"), "-cp", classpath,
      "spec.DisposeGuardChild", dir.toString, manager, op)
      .redirectErrorStream(true)
      .start()
    val output = new String(process.getInputStream.readAllBytes())
    process.waitFor(120, TimeUnit.SECONDS) shouldBe true
    withClue(output)(process.exitValue() shouldBe 0)
    output
  }

  private val ops = List("get-held", "exists-held", "count-held", "upsert-stream-held", "get-new", "upsert-new", "iterate-held")

  for manager <- List("shared", "own") do {
    s"A disposed $manager RocksDB store" should {
      ops.foreach { op =>
        s"refuse $op with StoreDisposedException" in {
          val output = runChild(manager, op)
          withClue(output)(output should include(s"RESULT $op refused"))
        }
      }
      "let concurrent readers, writers and an open iterator end cleanly when disposed under them" in {
        val output = runChild(manager, "concurrent")
        withClue(output)(output should include("RESULT concurrent clean"))
      }
    }
  }
}

case class GuardDoc(value: String, _id: Id[GuardDoc] = GuardDoc.id()) extends Document[GuardDoc]

object GuardDoc extends DocumentModel[GuardDoc] with JsonConversion[GuardDoc] {
  override implicit val rw: RW[GuardDoc] = RW.gen

  val value: F[String] = field("value", _.value)
}

class DisposeGuardDB(dir: Path, manager: String) extends LightDB {
  override type SM = StoreManager
  override val storeManager: StoreManager = manager match {
    case "shared" => RocksDBSharedStore(dir.resolve("shared"))
    case _ => RocksDBStore
  }
  override def directory: Option[Path] = Some(dir)
  override def upgrades: List[DatabaseUpgrade] = Nil
  override protected def disposeOnShutdown: Boolean = false

  val docs: Store[GuardDoc, GuardDoc.type] = store(GuardDoc)()
}

object DisposeGuardChild {
  private def outcome(task: Task[?]): String = task.attempt.sync() match {
    case scala.util.Success(_) => "ok"
    case scala.util.Failure(_: StoreDisposedException) => "refused"
    case scala.util.Failure(t) =>
      t.printStackTrace()
      s"error:${t.getClass.getName}"
  }

  def main(args: Array[String]): Unit = {
    val dir = Path.of(args(0))
    val db = new DisposeGuardDB(dir, args(1))
    val op = args(2)
    db.init.sync()
    val ids = (0 until 5000).map(i => Id[GuardDoc](s"doc-$i")).toList
    db.docs.transaction(_.upsert(ids.map(id => GuardDoc(id.value, id)))).sync()

    val result = op match {
      case "concurrent" => concurrent(db, ids)
      case _ =>
        val held = db.docs.transaction.create().sync()
        val iterator = held.jsonStream.evalMap(json => Task.sleep(1.millis).map(_ => json))
        val iterating = if op == "iterate-held" then Some(iterator.count.start()) else None
        if op == "iterate-held" then Task.sleep(100.millis).sync()
        db.dispose.sync()
        op match {
          case "get-held" => outcome(held.get(ids.head))
          case "exists-held" => outcome(held.exists(ids.head))
          case "count-held" => outcome(held.count)
          case "upsert-stream-held" => outcome(held.upsert(rapid.Stream.emits(List(GuardDoc("late")))))
          case "get-new" => outcome(db.docs.transaction(_.get(ids.head)))
          case "upsert-new" => outcome(db.docs.transaction(_.upsert(GuardDoc("late"))))
          case "iterate-held" => outcome(Task(iterating.get.sync()))
        }
    }
    println(s"RESULT $op $result")
    System.out.flush()
    System.exit(0)
  }

  /** Readers, writers and an iterator keep running while the database is disposed; each must end ok or refused. */
  private def concurrent(db: DisposeGuardDB, ids: List[Id[GuardDoc]]): String = {
    val stop = new AtomicBoolean(false)
    val ops = new AtomicInteger(0)
    def loop(f: Int => Task[?]): Task[String] = Task.defer {
      var i = 0
      var result: Option[String] = None
      while result.isEmpty && !stop.get() do {
        outcome(f(i)) match {
          case "ok" => ops.incrementAndGet()
          case other => result = Some(other)
        }
        i += 1
      }
      Task.pure(result.getOrElse("ok"))
    }
    val held = db.docs.transaction.create().sync()
    val workers = List(
      Task(loop(i => held.get(ids(i % ids.size))).sync()),
      Task(loop(i => db.docs.transaction(_.get(ids(i % ids.size)))).sync()),
      Task(loop(i => db.docs.transaction(_.upsert(GuardDoc(s"w$i")))).sync()),
      Task(loop(_ => held.upsert(rapid.Stream.emits(List(GuardDoc("bulk"))))).sync()),
      Task(outcome(held.jsonStream.evalMap(json => Task.sleep(1.millis).map(_ => json)).count))
    ).map(_.start())
    Task.sleep(300.millis).sync()
    val disposed = outcome(db.dispose)
    stop.set(true)
    val results = workers.map(_.sync())
    println(s"CONCURRENT ops=${ops.get()} dispose=$disposed workers=${results.mkString(",")}")
    if disposed == "ok" && results.forall(r => r == "ok" || r == "refused") then "clean" else "unclean"
  }
}
