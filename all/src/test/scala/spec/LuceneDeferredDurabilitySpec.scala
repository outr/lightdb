package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.lucene.index.Index
import lightdb.lucene.{LuceneDurability, LuceneStore, LuceneStoreManager}
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.{QueuedUpdateHandler, SplitCollection, SplitStoreManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.*

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Deferred durability on a RocksDB + Lucene split collection: a read commits nothing, a committed write is searchable
 * before any durable commit, the timer makes it durable and lifts the marker, a rollback restores the index to what
 * storage holds without touching other transactions' changes, and an index left with its marker down is rebuilt from
 * storage when it opens, whatever durability it then has.
 */
@EmbeddedTest
class LuceneDeferredDurabilitySpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  case class Item(name: String,
                  created: Timestamp = Timestamp(),
                  modified: Timestamp = Timestamp(),
                  _id: Id[Item] = Item.id()) extends RecordDocument[Item]

  object Item extends RecordDocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  class DB(dir: Path, durability: LuceneDurability) extends LightDB {
    override type SM = SplitStoreManager[RocksDBStore.type, LuceneStoreManager]
    override val storeManager: SM = SplitStoreManager(RocksDBStore, LuceneStore.withDurability(durability))
    override def name: String = "LuceneDeferredDurabilitySpec"
    override lazy val directory: Option[Path] = Some(dir)
    val items: SplitCollection[Item, Item.type, RocksDBStore[Item, Item.type], LuceneStore[Item, Item.type]] = store(Item)()
    override def upgrades: List[DatabaseUpgrade] = Nil

    def index: Index = items.searching.index
    def marker: Path = items.searching.path.get.resolve(Index.UncommittedMarker)
  }

  private val root = Path.of("db/LuceneDeferredDurabilitySpec")
  private def wipe(p: Path): Unit =
    if Files.exists(p) then Files.walk(p).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
  wipe(root)

  private def deferred(interval: FiniteDuration): LuceneDurability = LuceneDurability.Deferred(interval)

  private val db = new DB(root.resolve("main"), deferred(1.hour))

  /** Every stored item's name and every indexed one; a search by each stored name must find exactly the indexed ones. */
  private def agreement(d: DB): Task[(List[String], List[String])] = d.items.transaction { tx =>
    for
      stored <- tx.storage.stream.toList.map(_.map(_.name).sorted)
      indexed <- tx.query.toList.map(_.map(_.name).sorted)
      searched <- stored.distinct.map(n => tx.query.filter(_.name === n).toList.map(_.map(_.name))).tasks.map(_.flatten.sorted)
    yield {
      assert(searched == indexed, s"searching each stored name found $searched, the index holds $indexed")
      (stored, indexed)
    }
  }

  private def names(d: DB): Task[Set[String]] = d.items.transaction(_.query.toList.map(_.map(_.name).toSet))

  private def eventually(timeout: FiniteDuration)(condition: => Boolean): Task[Boolean] = Task.defer {
    val deadline = System.currentTimeMillis() + timeout.toMillis
    def loop: Task[Boolean] =
      if condition then Task.pure(true)
      else if System.currentTimeMillis() > deadline then Task.pure(false)
      else Task.sleep(25.millis).next(loop)
    loop
  }

  "Deferred durability" should {
    "initialize" in db.init.map { _ =>
      db.index.deferred shouldBe true
    }
    "run no commit and leave no marker for a transaction that only read" in {
      val before = db.index.commitCount
      names(db).map { found =>
        found shouldBe empty
        db.index.commitCount shouldBe before
        Files.exists(db.marker) shouldBe false
      }
    }
    "make a committed write searchable before any durable commit, with the marker down" in {
      val before = db.index.commitCount
      for
        _ <- db.items.transaction(_.insert(List(Item("alpha"), Item("beta"), Item("gamma"))))
        found <- db.items.transaction(_.query.filter(_.name === "beta").count)
      yield {
        found shouldBe 1
        db.index.commitCount shouldBe before
        Files.exists(db.marker) shouldBe true
      }
    }
    "commit durably on request and lift the marker once nothing is pending" in {
      val before = db.index.commitCount
      Task(db.index.commit()).map { _ =>
        db.index.commitCount shouldBe before + 1
        Files.exists(db.marker) shouldBe false
      }
    }
    "keep the marker while a transaction holds changes not yet published" in {
      for
        open <- db.items.transaction.create()
        _ <- open.insert(Item("delta"))
        _ <- open.flush
        _ <- Task(db.index.commit())
        whileOpen = Files.exists(db.marker)
        _ <- db.items.transaction.release(open)
        _ <- Task(db.index.commit())
      yield {
        whileOpen shouldBe true
        Files.exists(db.marker) shouldBe false
      }
    }
    "restore what a failed transaction touched from storage, leaving other transactions' changes alone" in {
      for
        other <- db.items.transaction.create()
        _ <- other.insert(Item("epsilon"))
        failed <- db.items.transaction { tx =>
          tx.upsert(Item("zeta")).flatMap(_ => tx.flush).flatMap(_ => Task.error[Unit](new RuntimeException("abort")))
        }.attempt
        _ <- db.items.transaction.release(other)
        (stored, indexed) <- agreement(db)
      yield {
        failed.isFailure shouldBe true
        indexed should contain allOf ("alpha", "beta", "gamma", "delta", "epsilon")
        indexed shouldBe stored
      }
    }
    "restore an index change that never reached the index before the rollback" in {
      for
        failed <- db.items.transaction { tx =>
          tx.searchUpdateHandler = QueuedUpdateHandler(tx)
          tx.upsert(Item("eta")).flatMap(_ => tx.flush).flatMap(_ => Task.error[Unit](new RuntimeException("abort")))
        }.attempt
        (stored, indexed) <- agreement(db)
      yield {
        failed.isFailure shouldBe true
        indexed shouldBe stored
      }
    }
    "restore the whole index after a rolled-back truncate" in {
      val before = Set("alpha", "beta", "gamma", "delta", "epsilon")
      for
        _ <- names(db).map(_ should contain allElementsOf before)
        failed <- db.items.transaction { tx =>
          tx.searching.truncate.flatMap(_ => Task.error[Unit](new RuntimeException("abort")))
        }.attempt
        (stored, indexed) <- agreement(db)
      yield {
        failed.isFailure shouldBe true
        indexed.toSet should contain allElementsOf before
        indexed shouldBe stored
      }
    }
    "make changes durable on the timer" in {
      val timed = new DB(root.resolve("timed"), deferred(100.millis))
      for
        _ <- timed.init
        before = timed.index.commitCount
        _ <- timed.items.transaction(_.insert(Item("theta")))
        markedAfterWrite = Files.exists(timed.marker)
        lifted <- eventually(10.seconds)(!Files.exists(timed.marker))
        commits = timed.index.commitCount - before
        _ <- timed.dispose
      yield {
        markedAfterWrite shouldBe true
        lifted shouldBe true
        commits should be >= 1L
      }
    }
    "stay in step with storage under concurrent writers, failures and constant durable commits" in {
      val busy = new DB(root.resolve("busy"), deferred(1.millis))
      // Each writer owns its documents: two transactions racing on one document can leave storage and index in either
      // order whatever the durability, which is not what this checks.
      def writer(w: Int): Task[Unit] = (1 to 150).foldLeft(Task.unit) { (previous, i) =>
        previous.next {
          busy.items.transaction { tx =>
            tx.upsert(Item(s"w$w-$i", _id = Id[Item](s"w$w-${i % 20}")))
              .next(if i % 4 == 0 then tx.delete(Id[Item](s"w$w-${(i + 7) % 20}")).unit else Task.unit)
              .next(tx.flush)
              .next(if i % 9 == 0 then Task.error[Unit](new RuntimeException("abort")) else Task.unit)
          }.attempt.unit
        }
      }
      // Searches only: loading a found document from storage can race a concurrent delete in any split collection.
      def reader: Task[Unit] = (1 to 300).foldLeft(Task.unit) { (previous, _) =>
        previous.next(busy.items.transaction(_.query.filter(_.name.startsWith("w")).count).unit)
      }
      for
        _ <- busy.init
        _ <- ((1 to 8).map(writer).toList :+ reader).tasksPar
        (stored, indexed) <- agreement(busy)
        _ <- busy.dispose
      yield {
        stored.size should be > 0
        indexed shouldBe stored
        Files.exists(busy.marker) shouldBe false
      }
    }
    "lift the marker at disposal" in db.dispose.map { _ =>
      Files.exists(db.marker) shouldBe false
    }
    "rebuild an index opened with its marker down from storage" in {
      val dir = root.resolve("behind")
      val first = new DB(dir, deferred(1.hour))
      for
        _ <- first.init
        _ <- first.items.transaction(_.insert(List(Item("one"), Item("two"))))
        // Storage moves on without the index: the index is now behind.
        _ <- first.items.transaction { tx =>
          tx.disableSearchUpdate()
          tx.insert(Item("three")).unit
        }
        behind <- names(first)
        _ <- first.dispose
        _ = Files.write(first.marker, Array.emptyByteArray)
        reopened <- Task(new DB(dir, deferred(1.hour)))
        _ <- reopened.init
        (stored, indexed) <- agreement(reopened)
        markedAfterRebuild = Files.exists(reopened.marker)
        _ <- reopened.dispose
      yield {
        behind shouldBe Set("one", "two")
        stored shouldBe List("one", "three", "two")
        indexed shouldBe stored
        markedAfterRebuild shouldBe false
      }
    }
    "rebuild a marked index even when the store now commits immediately" in {
      val dir = root.resolve("behind-immediate")
      val first = new DB(dir, deferred(1.hour))
      for
        _ <- first.init
        _ <- first.items.transaction { tx =>
          tx.disableSearchUpdate()
          tx.insert(List(Item("four"), Item("five"))).unit
        }
        _ <- first.dispose
        _ = Files.write(first.marker, Array.emptyByteArray)
        reopened <- Task(new DB(dir, LuceneDurability.Immediate))
        _ <- reopened.init
        (stored, indexed) <- agreement(reopened)
        deferredAfter = reopened.index.deferred
        markedAfterRebuild = Files.exists(reopened.marker)
        _ <- reopened.dispose
      yield {
        deferredAfter shouldBe false
        indexed shouldBe stored
        stored shouldBe List("five", "four")
        markedAfterRebuild shouldBe false
      }
    }
  }
}
