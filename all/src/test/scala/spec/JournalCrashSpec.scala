package spec

import lightdb.id.Id
import lightdb.store.journal.Journal
import lightdb.transaction.Transaction
import lightdb.trigger.StoreTrigger
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.*

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import java.net.URLClassLoader
import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Kills a process at each step of a journalled commit (the process halts itself: no shutdown hook, no disposal) and
 * reopens the database, over SQLite and over RocksDB. A kill before the commit-intent record is written leaves none of
 * the commit; a kill at any later step leaves, after reopen, every op of the owner and of the transactions joined to it
 * applied, the intent gone, and no store trigger fired by the replay (a reverse-edge row is written once, from the
 * intent). An intent replayed twice leaves the same rows, and two concurrent commits to one id end with the later
 * commit's value after a kill before either was applied.
 */
@EmbeddedTest
class JournalCrashSpec extends AnyWordSpec with Matchers {
  private val root = Path.of("db/JournalCrashSpec")
  if Files.exists(root) then Files.walk(root).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
  Files.createDirectories(root)

  private lazy val classpath: String = {
    val loaders = Iterator.iterate(getClass.getClassLoader)(_.getParent).takeWhile(_ != null).toList
    val urls = loaders.flatMap {
      case u: URLClassLoader => u.getURLs.toList.map(url => Path.of(url.toURI).toString)
      case _ => Nil
    }
    val own = Path.of(classOf[JournalCrashDB].getProtectionDomain.getCodeSource.getLocation.toURI).toString
    (own :: urls ++ System.getProperty("java.class.path").split(File.pathSeparator).toList).distinct.mkString(File.pathSeparator)
  }

  /** Runs the writer to its halt; returns its exit code. */
  private def run(dir: Path, backend: String, scenario: String, step: String): Int = {
    val javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString
    val log = dir.resolveSibling(s"${dir.getFileName}-$scenario-$step.log").toFile
    val process = new ProcessBuilder(javaBin, "-Xmx512m", "--enable-native-access=ALL-UNNAMED", "-cp", classpath,
      "spec.JournalCrashWriter", dir.toString, backend, scenario, step)
      .redirectErrorStream(true)
      .start()
    val reader = new Thread(() => {
      val in = new BufferedReader(new InputStreamReader(process.getInputStream))
      val out = new PrintWriter(log)
      try {
        var line = in.readLine()
        while line != null do {
          out.println(line)
          line = in.readLine()
        }
      } finally out.close()
    })
    reader.start()
    withClue(s"the writer did not finish (see $log)")(process.waitFor(120, TimeUnit.SECONDS) shouldBe true)
    reader.join(10_000)
    process.exitValue()
  }

  /** Counts every write trigger a store fires while the database opens. */
  private def counting[D <: Document[D], M <: DocumentModel[D]](store: Store[D, M], count: AtomicInteger): Unit =
    store.trigger += new StoreTrigger[D, M] {
      override def insert(doc: D, transaction: Transaction[D, M]): Task[Unit] = Task(count.incrementAndGet()).unit
      override def upsert(doc: D, transaction: Transaction[D, M]): Task[Unit] = Task(count.incrementAndGet()).unit
      override def delete(id: Id[D], transaction: Transaction[D, M]): Task[Unit] = Task(count.incrementAndGet()).unit
    }

  case class State(people: Map[String, String], tags: Set[String], links: Int, reversed: Int, unfinished: Int, triggers: Int)

  private def reopen(dir: Path, backend: String): State = {
    val db = new JournalCrashDB(dir, backend)
    val triggers = new AtomicInteger(0)
    counting(db.people, triggers)
    counting(db.tags, triggers)
    counting(db.links, triggers)
    counting(db.reversed, triggers)
    db.init.sync()
    try {
      val firedOpening = triggers.get()
      State(
        people = db.people.transaction(_.stream.toList).sync().map(p => p._id.value -> p.name).toMap,
        tags = db.tags.transaction(_.stream.toList).sync().map(_._id.value).toSet,
        links = db.links.transaction(_.count).sync(),
        reversed = db.reversed.transaction(_.count).sync(),
        unfinished = db.journal.unfinishedIntents.sync(),
        triggers = firedOpening
      )
    } finally db.dispose.sync()
  }

  private val committed = Map("p1" -> "one", "p2" -> "two")

  List("sqlite", "rocks").foreach { backend =>
    s"A journal over $backend killed mid-commit" should {
      "leave none of a commit killed before its intent was written" in {
        val dir = root.resolve(s"$backend-before-intent")
        run(dir, backend, "group", "BeforeIntent") shouldBe JournalCrashWriter.Exit
        val state = reopen(dir, backend)
        state.people shouldBe Map("seeded" -> "seeded")
        state.tags shouldBe empty
        (state.links, state.reversed, state.unfinished, state.triggers) shouldBe (0, 0, 0, 0)
      }
      List("AfterIntent", "BeforeApply", "AfterApplyStore", "BeforeDelete", "AfterDelete").foreach { step =>
        s"apply every op of the commit after a kill at $step" in {
          val dir = root.resolve(s"$backend-$step")
          run(dir, backend, "group", step) shouldBe JournalCrashWriter.Exit
          val state = reopen(dir, backend)
          state.people shouldBe committed
          state.tags shouldBe Set("t1")
          (state.links, state.reversed) shouldBe (1, 1)
          state.unfinished shouldBe 0
          withClue("store triggers fired by the replay:")(state.triggers shouldBe 0)
        }
      }
      "leave the same rows when an intent is replayed twice" in {
        val dir = root.resolve(s"$backend-replayed-twice")
        run(dir, backend, "group", "AfterIntent") shouldBe JournalCrashWriter.Exit
        // The reopen applies the intent and is killed before deleting it, so the next reopen applies it again.
        run(dir, backend, "open", "BeforeDelete") shouldBe JournalCrashWriter.Exit
        val state = reopen(dir, backend)
        state.people shouldBe committed
        state.tags shouldBe Set("t1")
        (state.links, state.reversed, state.unfinished, state.triggers) shouldBe (1, 1, 0, 0)
      }
      "end two concurrent commits to one id with the later commit's value, killed before either applied" in {
        val dir = root.resolve(s"$backend-order")
        run(dir, backend, "order", "BeforeApply") shouldBe JournalCrashWriter.Exit
        val state = reopen(dir, backend)
        state.people.get("same") shouldBe Some("second")
        state.unfinished shouldBe 0
      }
    }
  }
}
