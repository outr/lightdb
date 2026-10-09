package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.{JoinRefusedException, StoreFailedException, TransactionBoundExceededException}
import lightdb.id.Id
import lightdb.store.hashmap.HashMapStore
import lightdb.store.journal.{Journal, JournalRetry, JournalStep, JournalStore, JournalStoreManager}
import lightdb.store.{Store, StoreManager}
import lightdb.transaction.Transaction
import lightdb.trigger.StoreTrigger
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.{Task, taskSeq2Ops}

import java.nio.file.Path
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/**
 * The journal wrapper's in-process behaviour, over a map store: a joined transaction commits and rolls back with its
 * owner, a failure in it aborts the owner, the op bound (declared or derived from the record size) fails loud and
 * writes nothing, an apply error after the intent is durable leaves the commit successful and the rows present after
 * the in-process retry, exhausted retries make reads fail loud, and two commits to one id end with the later value.
 * The crash cases (a kill at each step, then a reopen) are in the `all` module, over durable stores.
 */
@EmbeddedTest
class JournalJoinSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach {
  import JournalJoinSpec.*

  override protected def afterEach(): Unit = Journal.faultHook = _ => ()

  private def withDB[T](retry: JournalRetry = JournalRetry(attempts = 3, initialDelay = 10.millis))(f: JDB => T): T = {
    val db = new JDB(JournalStoreManager(HashMapStore, recordBytes = Some(64L * 1024L), defaultMaxOpBytes = 1024L,
      maxOpBytes = Map("Big" -> 64L * 1024L), retry = retry))
    db.init.sync()
    try f(db) finally db.dispose.sync()
  }

  "A journalled store" should {
    "commit a joined transaction's writes with its owner, and neither before" in withDB() { db =>
      val seen = db.items.transaction { owner =>
        owner.insert(Item("a", Id("a"))).next(db.tags.transaction.joining(owner) { joined =>
          joined.insert(Tag("t", Id("t")))
        }).next(db.tags.transaction(_.get(Id[Tag]("t")))).flatMap(tag =>
          db.items.transaction(_.get(Id[Item]("a"))).map(item => (item, tag)))
      }.sync()
      seen shouldBe (None, None)
      db.items.transaction(_.get(Id[Item]("a"))).sync().map(_.name) shouldBe Some("a")
      db.tags.transaction(_.get(Id[Tag]("t"))).sync().map(_.name) shouldBe Some("t")
    }
    "read its own buffered write back by id" in withDB() { db =>
      db.items.transaction { tx =>
        tx.insert(Item("mine", Id("mine"))).next(tx.get(Id[Item]("mine")))
      }.sync().map(_.name) shouldBe Some("mine")
    }
    "roll a joined transaction back with its owner" in withDB() { db =>
      val result = db.items.transaction { owner =>
        owner.insert(Item("b", Id("b"))).next(db.tags.transaction.joining(owner) { joined =>
          joined.insert(Tag("u", Id("u")))
        }).next(Task.error[Unit](new RuntimeException("owner fails")))
      }.attempt.sync()
      result.isFailure shouldBe true
      db.items.transaction(_.get(Id[Item]("b"))).sync() shouldBe None
      db.tags.transaction(_.get(Id[Tag]("u"))).sync() shouldBe None
    }
    "abort the owner when the joined transaction fails" in withDB() { db =>
      val result = db.items.transaction { owner =>
        owner.insert(Item("c", Id("c"))).next(db.tags.transaction.joining(owner) { joined =>
          joined.insert(Tag("v", Id("v"))).next(Task.error[Unit](new RuntimeException("joined fails")))
        }.attempt).map(_ => ())
      }.attempt.sync()
      result.isFailure shouldBe true
      db.items.transaction(_.get(Id[Item]("c"))).sync() shouldBe None
      db.tags.transaction(_.get(Id[Tag]("v"))).sync() shouldBe None
    }
    "tell a joined transaction's triggers of the outcome before the owner's" in withDB() { db =>
      val events = new ConcurrentLinkedQueue[String]()
      db.items.trigger += new StoreTrigger[Item, Item.type] {
        override def transactionCommitted(transaction: Transaction[Item, Item.type]): Task[Unit] = Task(events.add("items committed")).unit
        override def transactionRolledBack(transaction: Transaction[Item, Item.type]): Task[Unit] = Task(events.add("items rolled back")).unit
      }
      db.tags.trigger += new StoreTrigger[Tag, Tag.type] {
        override def transactionCommitted(transaction: Transaction[Tag, Tag.type]): Task[Unit] = Task(events.add("tags committed")).unit
        override def transactionRolledBack(transaction: Transaction[Tag, Tag.type]): Task[Unit] = Task(events.add("tags rolled back")).unit
      }
      db.items.transaction { owner =>
        owner.insert(Item("d", Id("d"))).next(db.tags.transaction.joining(owner)(_.insert(Tag("w", Id("w"))))).unit
      }.sync()
      db.items.transaction { owner =>
        db.tags.transaction.joining(owner)(_.insert(Tag("x", Id("x")))).next(Task.error[Unit](new RuntimeException("no")))
      }.attempt.sync()
      events.asScala.toList shouldBe List("tags committed", "items committed", "tags rolled back", "items rolled back")
    }
    "refuse a join to a transaction of another manager" in withDB() { db =>
      val other = new PlainDB
      other.init.sync()
      try {
        val result = other.items.transaction { owner =>
          db.tags.transaction.joining(owner)(_.insert(Tag("y", Id("y")))).unit
        }.attempt.sync()
        result.failed.get shouldBe a[JoinRefusedException]
      } finally other.dispose.sync()
    }
    "fail loud past a declared bound and write nothing" in withDB() { db =>
      val result = db.items.transaction.withGroupCommit(10) { tx =>
        (1 to 11).toList.map(i => tx.insert(Item(s"n$i", Id(s"n$i")))).tasks.unit
      }.attempt.sync()
      val error = result.failed.get
      error shouldBe a[TransactionBoundExceededException]
      error.getMessage should include("Item")
      error.getMessage should include("11")
      db.items.transaction(_.count).sync() shouldBe 0
    }
    "count a joined transaction's writes against the owner's bound" in withDB() { db =>
      val result = db.items.transaction.withGroupCommit(10) { owner =>
        (1 to 6).toList.map(i => owner.insert(Item(s"o$i", Id(s"o$i")))).tasks
          .next(db.tags.transaction.joining(owner) { joined =>
            (1 to 5).toList.map(i => joined.insert(Tag(s"j$i", Id(s"j$i")))).tasks
          }).unit
      }.attempt.sync()
      result.failed.get shouldBe a[TransactionBoundExceededException]
      db.items.transaction(_.count).sync() shouldBe 0
      db.tags.transaction(_.count).sync() shouldBe 0
    }
    "derive the bound from the record size and the store's op size" in withDB() { db =>
      // 64 KiB records, 1 KiB ops: a group commit holds 64.
      db.items.opBound shouldBe 64
      db.big.opBound shouldBe 1
      val result = db.items.transaction.withGroupCommit() { tx =>
        (1 to 65).toList.map(i => tx.insert(Item(s"p$i", Id(s"p$i")))).tasks.unit
      }.attempt.sync()
      result.failed.get shouldBe a[TransactionBoundExceededException]
      db.items.transaction(_.count).sync() shouldBe 0
      db.items.transaction.withGroupCommit() { tx =>
        (1 to 64).toList.map(i => tx.insert(Item(s"p$i", Id(s"p$i")))).tasks.unit
      }.sync()
      db.items.transaction(_.count).sync() shouldBe 64
    }
    "refuse a write larger than the store's declared op size" in withDB() { db =>
      val result = db.items.transaction(_.insert(Item("x" * 2048, Id("huge")))).attempt.sync()
      result.failed.get shouldBe an[IllegalArgumentException]
      db.items.transaction(_.get(Id[Item]("huge"))).sync() shouldBe None
    }
    "keep a commit successful through an apply error after the intent, with the rows applied by the retry" in withDB() { db =>
      val failures = new AtomicInteger(1)
      Journal.faultHook = {
        case JournalStep.BeforeApply(_, _) if failures.getAndDecrement() > 0 => throw new RuntimeException("injected apply error")
        case _ => ()
      }
      db.items.transaction { owner =>
        owner.insert(Item("e", Id("e"))).next(db.tags.transaction.joining(owner)(_.insert(Tag("z", Id("z"))))).unit
      }.sync()
      db.items.transaction(_.get(Id[Item]("e"))).sync().map(_.name) shouldBe Some("e")
      db.tags.transaction(_.get(Id[Tag]("z"))).sync().map(_.name) shouldBe Some("z")
      db.items.journal.pending shouldBe 0
    }
    "fail reads loud once the apply retries run out, and refuse new commits" in withDB(JournalRetry(attempts = 2, initialDelay = 5.millis)) { db =>
      Journal.faultHook = {
        case JournalStep.BeforeApply(_, _) => throw new RuntimeException("persistent apply error")
        case _ => ()
      }
      db.items.transaction(_.insert(Item("f", Id("f")))).sync()
      db.items.journal.failure should not be empty
      db.items.transaction(_.get(Id[Item]("f"))).attempt.sync().failed.get shouldBe a[StoreFailedException]
      db.items.transaction(_.insert(Item("g", Id("g")))).attempt.sync().failed.get shouldBe a[StoreFailedException]
    }
    "end two concurrent commits to one id with the later commit's value" in withDB() { db =>
      val secondWritten = new CountDownLatch(1)
      Journal.faultHook = {
        case JournalStep.AfterIntent(2) => secondWritten.countDown()
        case JournalStep.BeforeApply(1, 1) => secondWritten.await(10, TimeUnit.SECONDS)
        case _ => ()
      }
      val first = new Thread(() => { db.items.transaction(_.upsert(Item("first", Id("same")))).sync(); () })
      first.start()
      Thread.sleep(200)
      db.items.transaction(_.upsert(Item("second", Id("same")))).sync()
      first.join(10_000)
      db.items.transaction(_.get(Id[Item]("same"))).sync().map(_.name) shouldBe Some("second")
    }
  }
}

object JournalJoinSpec {
  case class Item(name: String, _id: Id[Item] = Item.id()) extends Document[Item]

  object Item extends DocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  case class Tag(name: String, _id: Id[Tag] = Tag.id()) extends Document[Tag]

  object Tag extends DocumentModel[Tag] with JsonConversion[Tag] {
    override implicit val rw: RW[Tag] = RW.gen
  }

  case class Big(name: String, _id: Id[Big] = Big.id()) extends Document[Big]

  object Big extends DocumentModel[Big] with JsonConversion[Big] {
    override implicit val rw: RW[Big] = RW.gen
  }

  class JDB(manager: JournalStoreManager[HashMapStore.type]) extends LightDB {
    override type SM = JournalStoreManager[HashMapStore.type]
    override val storeManager: SM = manager
    override def name: String = "JournalJoinSpec"
    override lazy val directory: Option[Path] = None
    override protected def disposeOnShutdown: Boolean = false
    val items: JournalStore[Item, Item.type] = store(Item)()
    val tags: JournalStore[Tag, Tag.type] = store(Tag)()
    val big: JournalStore[Big, Big.type] = store(Big)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  class PlainDB extends LightDB {
    override type SM = HashMapStore.type
    override val storeManager: SM = HashMapStore
    override def name: String = "JournalJoinSpecPlain"
    override lazy val directory: Option[Path] = None
    override protected def disposeOnShutdown: Boolean = false
    val items: HashMapStore[Item, Item.type] = store(Item)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
