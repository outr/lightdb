package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.*

/**
 * `transaction.modify` is atomic within one process: concurrent calls on one record each read the row the previous one
 * committed, so none of their updates is lost. Each call runs in its own transaction on its own platform thread; all
 * threads are released together by one latch, and the record is read back only after every call has returned.
 *
 * Where the store also locks the row in the database, [[otherProcess]] opens a second database instance on the same
 * database with its own store manager and locks, standing in for another process, and the calls are split between the
 * two.
 */
abstract class AbstractConcurrentModifySpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  def storeManager: StoreManager

  /** Concurrent `modify` calls per test. */
  protected def writers: Int = 100

  /** Whether a rolled-back transaction's write is undone on this store. */
  protected def supportsRollback: Boolean = true

  /** Whether a read in a transaction sees that transaction's own uncommitted writes on this store. */
  protected def readsOwnWrites: Boolean = true

  /** A second store manager on the same database, standing in for another process, for a store that excludes other
    * processes. */
  protected def otherProcess: Option[StoreManager] = None

  override implicit protected val testTimeout: FiniteDuration = 5.minutes

  protected lazy val specName: String = getClass.getSimpleName

  private lazy val dbPath: Path = Path.of(s"db/$specName")

  private def deleteDirectoryIfExists(path: Path): Unit = if Files.exists(path) then {
    Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
  }

  case class Tally(items: List[String], _id: Id[Tally] = Tally.id()) extends Document[Tally]

  object Tally extends DocumentModel[Tally] with JsonConversion[Tally] {
    override implicit val rw: RW[Tally] = RW.gen

    val items: F[List[String]] = field("items", _.items)
  }

  class TallyDB(manager: StoreManager) extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = manager

    override def name: String = s"cm_${specName.toLowerCase}"

    override lazy val directory: Option[Path] = Some(dbPath)

    val tallies: Store[Tally, Tally.type] = store(Tally).withName(s"tally_${specName.toLowerCase}")()

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  object DB extends TallyDB(spec.storeManager)

  private object Deliberate extends RuntimeException("deliberate rollback")

  private def append(id: Id[Tally], i: Int, db: TallyDB = DB): Task[Option[Tally]] = db.tallies.transaction { tx =>
    tx.modify(id) {
      case Some(t) => Task.pure(Some(t.copy(items = t.items :+ i.toString)))
      case None => Task.pure(Some(Tally(List(i.toString), id)))
    }
  }

  /** Runs `body(i)` for i in 1..n, each on its own platform thread, all released at once. Returns the unexpected errors. */
  private def concurrently(n: Int)(body: Int => Unit): List[Throwable] = {
    val start = new CountDownLatch(1)
    val errors = new ConcurrentLinkedQueue[Throwable]
    val threads = (1 to n).toList.map { i =>
      val run: Runnable = () => {
        start.await()
        try body(i)
        catch {
          case t: Throwable => errors.add(t)
        }
        ()
      }
      val t = new Thread(run, s"$specName-modify-$i")
      t.setDaemon(true)
      t.start()
      t
    }
    start.countDown()
    val deadline = System.nanoTime() + 3.minutes.toNanos
    threads.foreach(t => t.join(math.max(1L, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime()))))
    val running = threads.count(_.isAlive)
    if running > 0 then fail(s"$running of $n modify calls had not returned after 3 minutes")
    errors.asScala.toList
  }

  private def read(id: Id[Tally]): Task[Tally] = DB.tallies.transaction(_.apply(id))

  specName should {
    "initialize the database" in {
      deleteDirectoryIfExists(dbPath)
      DB.init.succeed
    }
    "keep every update of concurrent modify calls on one record" in {
      val id = Tally.id("concurrent")
      for
        _ <- DB.tallies.transaction(_.upsert(Tally(Nil, id)))
        errors <- Task(concurrently(writers)(i => append(id, i).sync()))
        tally <- read(id)
      yield {
        errors shouldBe Nil
        withClue(s"kept ${tally.items.size} of $writers updates:") {
          tally.items.map(_.toInt).sorted shouldBe (1 to writers).toList
        }
      }
    }
    "create a missing record once under concurrent modify calls" in {
      val id = Tally.id("created")
      for
        errors <- Task(concurrently(writers)(i => append(id, i).sync()))
        tally <- read(id)
      yield {
        errors shouldBe Nil
        withClue(s"kept ${tally.items.size} of $writers updates:") {
          tally.items.map(_.toInt).sorted shouldBe (1 to writers).toList
        }
      }
    }
    "hand the next caller the committed row, not a rolled-back write" in {
      if !supportsRollback then cancel(s"$specName does not undo a rolled-back write")
      val id = Tally.id("rollback")
      for
        _ <- DB.tallies.transaction(_.upsert(Tally(Nil, id)))
        errors <- Task(concurrently(writers) { i =>
          if i % 2 == 0 then append(id, i).sync()
          else {
            val failed = DB.tallies.transaction { tx =>
              tx.modify(id)(t => Task.pure(t.map(t => t.copy(items = t.items :+ i.toString)))).flatMap(_ => Task.error(Deliberate))
            }.attempt.sync()
            if !failed.failed.toOption.contains(Deliberate) then throw new AssertionError(s"expected the deliberate failure, got $failed")
          }
        })
        tally <- read(id)
      yield {
        errors shouldBe Nil
        withClue(s"kept ${tally.items.size} of ${writers / 2} committed updates:") {
          tally.items.map(_.toInt).sorted shouldBe (2 to writers by 2).toList
        }
      }
    }
    "keep every update when another process modifies the same record" in {
      otherProcess match {
        case None => cancel(s"$specName does not exclude other processes")
        case Some(manager) =>
          val other = new TallyDB(manager)
          val id = Tally.id("processes")
          for
            _ <- other.init
            _ <- DB.tallies.transaction(_.upsert(Tally(Nil, id)))
            errors <- Task(concurrently(writers)(i => append(id, i, if i % 2 == 0 then DB else other).sync()))
            _ <- other.dispose
            tally <- read(id)
          yield {
            errors shouldBe Nil
            withClue(s"kept ${tally.items.size} of $writers updates:") {
              tally.items.map(_.toInt).sorted shouldBe (1 to writers).toList
            }
          }
      }
    }
    "modify one record twice in one transaction" in {
      val id = Tally.id("reentrant")
      for
        _ <- DB.tallies.transaction(_.upsert(Tally(Nil, id)))
        _ <- DB.tallies.transaction { tx =>
          tx.modify(id)(t => Task.pure(t.map(t => t.copy(items = t.items :+ "1"))))
            .flatMap(_ => tx.modify(id)(t => Task.pure(t.map(t => t.copy(items = t.items :+ "2")))))
        }
        tally <- read(id)
      yield {
        if readsOwnWrites then tally.items shouldBe List("1", "2")
        else tally.items should contain("2")
      }
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }
}
