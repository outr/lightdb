package spec

import lightdb.lucene.LuceneDurability
import lightdb.store.split.SplitCollection
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.*

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import java.net.URLClassLoader
import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

/**
 * Kills a process writing to a split collection whose index defers its durable commits (SIGKILL: no shutdown hook,
 * no disposal) and reopens the database: storage keeps every committed transaction, and the index is rebuilt to match
 * storage exactly. Covered: an index that never made anything durable (a long interval), one killed while durable
 * commits run constantly (a short interval), and one directory killed and reopened again and again.
 */
@EmbeddedTest
class LuceneDeferredDurabilityCrashSpec extends AnyWordSpec with Matchers {
  private val root = Path.of("db/LuceneDeferredDurabilityCrashSpec")
  if Files.exists(root) then Files.walk(root).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
  Files.createDirectories(root)

  private val random = new scala.util.Random(42)

  // The test classes and their dependencies: a forked test JVM may load them through its own class loaders rather
  // than its class path.
  private lazy val classpath: String = {
    val loaders = Iterator.iterate(getClass.getClassLoader)(_.getParent).takeWhile(_ != null).toList
    val urls = loaders.flatMap {
      case u: URLClassLoader => u.getURLs.toList.map(url => Path.of(url.toURI).toString)
      case _ => Nil
    }
    val own = Path.of(classOf[CrashDB].getProtectionDomain.getCodeSource.getLocation.toURI).toString
    (own :: urls ++ System.getProperty("java.class.path").split(File.pathSeparator).toList).distinct.mkString(File.pathSeparator)
  }

  /** Runs the writer until it has committed at least `commits` numbered transactions past `first`, then kills it.
    * Returns the last transaction it reported committed. */
  private def writeAndKill(dir: Path, intervalMs: Long, first: Int, commits: Int): Int = {
    val javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString
    val log = dir.resolveSibling(s"${dir.getFileName}-writer-$first.log").toFile
    val process = new ProcessBuilder(javaBin, "-Xmx512m", "-cp", classpath,
      "spec.CrashWriter", dir.toString, intervalMs.toString, first.toString)
      .redirectErrorStream(true)
      .start()
    val last = new AtomicInteger(first - 1)
    val reader = new Thread(() => {
      val in = new BufferedReader(new InputStreamReader(process.getInputStream))
      val out = new PrintWriter(log)
      try {
        var line = in.readLine()
        while line != null do {
          out.println(line)
          if line.startsWith("COMMITTED ") then last.set(line.drop(10).trim.toInt)
          line = in.readLine()
        }
      } finally out.close()
    })
    reader.start()
    val deadline = System.currentTimeMillis() + 180_000L
    while last.get() < first + commits && process.isAlive && System.currentTimeMillis() < deadline do Thread.sleep(5)
    withClue(s"the writer committed ${last.get() - first + 1} transactions (see $log)") {
      process.isAlive shouldBe true
      last.get() should be >= first + commits
    }
    Thread.sleep(random.nextInt(40).toLong)
    process.destroyForcibly()
    process.waitFor(30, TimeUnit.SECONDS) shouldBe true
    reader.join(10_000)
    last.get()
  }

  /** Reopens the database and checks it: the index holds exactly what storage holds, and every numbered document
    * whose transactions all committed is in storage as expected. Returns whether the index had to be rebuilt. */
  private def reopenAndVerify(dir: Path, first: Int, last: Int): Boolean = {
    val db = new CrashDB(dir, LuceneDurability.Deferred(1.hour))
    // Off while opening: the count check it enables would re-index an index that is behind by a different number of
    // documents, hiding whether the marker did its job.
    val reIndexWhenOutOfSync = SplitCollection.ReIndexWhenOutOfSync
    SplitCollection.ReIndexWhenOutOfSync = false
    try db.init.sync() finally SplitCollection.ReIndexWhenOutOfSync = reIndexWhenOutOfSync
    val rebuilt = try {
      val (stored, indexed, searched) = db.items.transaction { tx =>
        for
          stored <- tx.storage.stream.toList
          indexed <- tx.query.toList
          searched <- stored.map(_.name).distinct.map(n => tx.query.filter(_.name === n).toList).tasks.map(_.flatten)
        yield (stored.map(i => i._id.value -> i.name).toMap, indexed.map(i => i._id.value -> i.name).toMap,
          searched.map(i => i._id.value -> i.name).toMap)
      }.sync()
      withClue("index against storage:") {
        indexed shouldBe stored
        searched shouldBe stored
      }
      val textHits = db.items.transaction(_.query.filter(_.text.words("text")).count).sync()
      textHits shouldBe stored.size
      (first to last - 2).foreach { j =>
        withClue(s"d$j after transaction $last committed:") {
          stored.get(s"d$j") shouldBe CrashItem.expectedName(j)
        }
      }
      db.index.recoveredDirty
    } finally db.dispose.sync()
    withClue("the marker after a clean disposal:")(Files.exists(db.marker) shouldBe false)
    rebuilt
  }

  private def marked(dir: Path): Boolean = Files.exists(dir.resolve("CrashItem").resolve("search").resolve(lightdb.lucene.index.Index.UncommittedMarker))

  "A deferred-durability index killed mid-write" should {
    "be rebuilt when nothing it held was ever durable" in {
      val dir = root.resolve("never-durable")
      val last = writeAndKill(dir, intervalMs = 3_600_000L, first = 1, commits = 300)
      marked(dir) shouldBe true
      reopenAndVerify(dir, 1, last) shouldBe true
    }
    "be rebuilt when killed while durable commits run constantly" in {
      val dir = root.resolve("constant-commits")
      val last = writeAndKill(dir, intervalMs = 2L, first = 1, commits = 600)
      reopenAndVerify(dir, 1, last)
    }
    "come back each time it is killed and reopened" in {
      val dir = root.resolve("repeated")
      (1 to 4).foreach { round =>
        val first = round * 100_000 + 1
        val last = writeAndKill(dir, intervalMs = if round % 2 == 0 then 20L else 500L, first = first, commits = 150)
        withClue(s"round $round:")(reopenAndVerify(dir, first, last))
      }
    }
  }
}
