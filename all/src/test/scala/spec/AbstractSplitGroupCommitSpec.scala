package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.error.{DuplicateIdException, JoinRefusedException, StoreFailedException, TransactionBoundExceededException}
import lightdb.id.Id
import lightdb.lucene.index.Index
import lightdb.lucene.{LuceneDurability, LuceneStore, LuceneStoreManager}
import lightdb.rocksdb.{RocksDBSharedStore, RocksDBStore}
import lightdb.store.split.{IndexRetry, SplitCollection, SplitStoreManager}
import lightdb.transaction.Transaction
import lightdb.trigger.StoreTrigger
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.*

import java.nio.file.{Files, Path}
import java.util.Comparator
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters.*

/**
 * Group commit on split collections over one RocksDB database: a store that opted in holds a transaction's writes
 * (another transaction sees none of them, the transaction reads them back by id), a rollback discards them, a joined
 * transaction on another store commits in the owner's one `WriteBatch` or not at all, a failed batch leaves neither
 * store's rows, the op bound fails loud and writes nothing, an index error after the batch counts as committed and is
 * re-indexed in process (with the rebuild marker set meanwhile and an indexed notification after), and only once the
 * in-process and full re-index fail do queries fail loud. A store that did not opt in still writes through.
 */
abstract class AbstractSplitGroupCommitSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  import AbstractSplitGroupCommitSpec.*

  protected def durability: LuceneDurability

  private lazy val root = Path.of(s"db/${getClass.getSimpleName}")

  private def fresh(name: String): DB = {
    val dir = root.resolve(name)
    if Files.exists(dir) then Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
    new DB(dir, durability)
  }

  private lazy val db: DB = fresh("main")

  private def storedItem(d: DB, id: String): Task[Option[String]] = d.items.transaction(_.storage.get(Id[Item](id)).map(_.map(_.name)))
  private def indexedItem(d: DB, name: String): Task[Int] = d.items.transaction(_.query.filter(_.name === name).count)
  private def storedTag(d: DB, id: String): Task[Option[String]] = d.tags.transaction(_.storage.get(Id[Tag](id)).map(_.map(_.name)))
  private def indexedTag(d: DB, name: String): Task[Int] = d.tags.transaction(_.query.filter(_.name === name).count)

  private def eventually(timeout: FiniteDuration)(condition: => Task[Boolean]): Task[Boolean] = Task.defer {
    val deadline = System.currentTimeMillis() + timeout.toMillis
    def loop: Task[Boolean] = condition.flatMap {
      case true => Task.pure(true)
      case false if System.currentTimeMillis() > deadline => Task.pure(false)
      case false => Task.sleep(25.millis).next(loop)
    }
    loop
  }

  "Group commit on RocksDB split collections" should {
    "initialize" in db.init.map { _ =>
      db.items.supportsJoin shouldBe true
      db.items.groupCommitDefault shouldBe Some(20_000)
    }
    "hide a buffered row from other transactions until it commits, and read it back by id in its own" in {
      for
        owner <- db.items.transaction.create()
        _ <- owner.insert(Item("buffered", Id("buffered")))
        own <- owner.get(Id[Item]("buffered"))
        otherById <- storedItem(db, "buffered")
        otherByQuery <- indexedItem(db, "buffered")
        _ <- db.items.transaction.release(owner)
        afterById <- storedItem(db, "buffered")
        afterByQuery <- indexedItem(db, "buffered")
      yield {
        own.map(_.name) shouldBe Some("buffered")
        (otherById, otherByQuery) shouldBe (None, 0)
        (afterById, afterByQuery) shouldBe (Some("buffered"), 1)
      }
    }
    "discard a rolled-back transaction's writes: no storage row and no index row" in {
      for
        failed <- db.items.transaction(_.insert(Item("discarded", Id("discarded"))).next(Task.error[Unit](new RuntimeException("abort")))).attempt
        row <- storedItem(db, "discarded")
        hits <- indexedItem(db, "discarded")
      yield {
        failed.isFailure shouldBe true
        (row, hits) shouldBe (None, 0)
      }
    }
    "commit a joined transaction on another store in the owner's write, and neither before" in {
      for
        seen <- db.items.transaction { owner =>
          owner.insert(Item("owner-row", Id("owner-row")))
            .next(db.tags.transaction.joining(owner)(_.insert(Tag("joined-row", Id("joined-row")))))
            .next(storedTag(db, "joined-row"))
        }
        item <- storedItem(db, "owner-row")
        tag <- storedTag(db, "joined-row")
        itemHits <- indexedItem(db, "owner-row")
        tagHits <- indexedTag(db, "joined-row")
      yield {
        seen shouldBe None
        (item, tag) shouldBe (Some("owner-row"), Some("joined-row"))
        (itemHits, tagHits) shouldBe (1, 1)
      }
    }
    "roll a joined transaction back with its owner" in {
      for
        failed <- db.items.transaction { owner =>
          owner.insert(Item("owner-gone", Id("owner-gone")))
            .next(db.tags.transaction.joining(owner)(_.insert(Tag("joined-gone", Id("joined-gone")))))
            .next(Task.error[Unit](new RuntimeException("abort")))
        }.attempt
        item <- storedItem(db, "owner-gone")
        tag <- storedTag(db, "joined-gone")
        tagHits <- indexedTag(db, "joined-gone")
      yield {
        failed.isFailure shouldBe true
        (item, tag, tagHits) shouldBe (None, None, 0)
      }
    }
    "leave neither the owner's rows nor a joined one when the batch write fails" in {
      for
        _ <- db.items.transaction(_.insert(Item("existing", Id("existing"))))
        failed <- db.items.transaction { owner =>
          owner.insert(Item("beside-duplicate", Id("beside-duplicate")))
            .next(owner.insert(Item("duplicate", Id("existing"))))
            .next(db.tags.transaction.joining(owner)(_.insert(Tag("joined-beside", Id("joined-beside")))))
            .unit
        }.attempt
        item <- storedItem(db, "beside-duplicate")
        existing <- storedItem(db, "existing")
        tag <- storedTag(db, "joined-beside")
        itemHits <- indexedItem(db, "beside-duplicate")
        tagHits <- indexedTag(db, "joined-beside")
      yield {
        failed.failed.get shouldBe a[DuplicateIdException]
        existing shouldBe Some("existing")
        (item, tag, itemHits, tagHits) shouldBe (None, None, 0, 0)
      }
    }
    "refuse a join to a transaction that writes through" in {
      db.plain.transaction { owner =>
        db.tags.transaction.joining(owner)(_.insert(Tag("never", Id("never")))).unit
      }.attempt.map(_.failed.get shouldBe a[JoinRefusedException])
    }
    "fail loud at the 20,001st op of a group-commit transaction and write nothing" in {
      val docs = (1 to 20_001).toList.map(i => Item(s"bulk$i", Id(s"bulk$i")))
      for
        failed <- db.items.transaction(tx => docs.map(tx.insert).tasks.unit).attempt
        first <- storedItem(db, "bulk1")
      yield {
        val error = failed.failed.get
        error shouldBe a[TransactionBoundExceededException]
        error.getMessage should include("20001")
        first shouldBe None
      }
    }
    "fail loud when a joined transaction takes the group past the owner's bound" in {
      for
        failed <- db.items.transaction.withGroupCommit(10) { owner =>
          (1 to 6).toList.map(i => owner.insert(Item(s"b$i", Id(s"b$i")))).tasks
            .next(db.tags.transaction.joining(owner)(joined => (1 to 5).toList.map(i => joined.insert(Tag(s"jb$i", Id(s"jb$i")))).tasks))
            .unit
        }.attempt
        item <- storedItem(db, "b1")
        tag <- storedTag(db, "jb1")
      yield {
        failed.failed.get shouldBe a[TransactionBoundExceededException]
        (item, tag) shouldBe (None, None)
      }
    }
    "keep a store that did not opt in writing through" in {
      for
        open <- db.plain.transaction.create()
        _ <- open.insert(Plain("through", Id("through")))
        seen <- db.plain.transaction(_.get(Id[Plain]("through")))
        _ <- db.plain.transaction.release(open)
      yield seen.map(_.name) shouldBe Some("through")
    }
    "re-index a store holding more rows than its bound" in {
      for
        _ <- (0 until 3).toList.map(c => db.small.transaction(tx => (1 to 40).toList.map(i => tx.insert(Small(s"s$c-$i"))).tasks)).tasks
        reindexed <- db.small.reIndex()
        count <- db.small.transaction(_.query.count)
      yield {
        reindexed shouldBe true
        count shouldBe 120
      }
    }
    "count an index error after the batch as committed, and re-index the row in process" in {
      val heal = fresh("heal")
      val failNext = new AtomicInteger(0)
      val notified = new ConcurrentLinkedQueue[String]()
      heal.items.indexRetry = IndexRetry(attempts = 5, initialDelay = 500.millis)
      heal.items.searching.trigger += failing(failNext, new AtomicBoolean(false))
      heal.items.onIndexed(ids => Task(ids.foreach(id => notified.add(id.value))))
      val marker = heal.items.searching.path.get.resolve(Index.UncommittedMarker)
      for
        _ <- heal.init
        _ <- Task(failNext.set(1))
        _ <- heal.items.transaction(_.insert(Item("late", Id("late"))))
        byId <- storedItem(heal, "late")
        pending = heal.items.isIndexPending(Id[Item]("late"))
        markedMeanwhile = Files.exists(marker)
        found <- eventually(15.seconds)(indexedItem(heal, "late").map(_ == 1))
        markerLifted <- eventually(15.seconds)(Task(!Files.exists(marker)))
        _ <- heal.dispose
      yield {
        byId shouldBe Some("late")
        pending shouldBe true
        markedMeanwhile shouldBe true
        found shouldBe true
        notified.asScala.toList shouldBe List("late")
        heal.items.isIndexPending(Id[Item]("late")) shouldBe false
        markerLifted shouldBe true
      }
    }
    "fail queries loud once the in-process and the full re-index fail" in {
      val broken = fresh("broken")
      broken.items.indexRetry = IndexRetry(attempts = 2, initialDelay = 20.millis)
      val always = new AtomicBoolean(false)
      broken.items.searching.trigger += failing(new AtomicInteger(0), always)
      for
        _ <- broken.init
        _ <- Task(always.set(true))
        _ <- broken.items.transaction(_.insert(Item("stuck", Id("stuck"))))
        failedState <- eventually(15.seconds)(Task(broken.items.indexFailure.nonEmpty))
        query <- indexedItem(broken, "stuck").attempt
        byId <- storedItem(broken, "stuck")
        _ <- Task(always.set(false))
        _ <- broken.dispose
      yield {
        failedState shouldBe true
        query.failed.get shouldBe a[StoreFailedException]
        byId shouldBe Some("stuck")
      }
    }
    "dispose" in db.dispose.succeed
  }

  /** Fails the search index's next `count` writes, or every write while `always` is set. */
  private def failing(count: AtomicInteger, always: AtomicBoolean): StoreTrigger[Item, Item.type] = new StoreTrigger[Item, Item.type] {
    private def maybeFail: Task[Unit] = Task {
      if always.get() || count.getAndDecrement() > 0 then throw new RuntimeException("injected index failure")
    }
    override def insert(doc: Item, transaction: Transaction[Item, Item.type]): Task[Unit] = maybeFail
    override def upsert(doc: Item, transaction: Transaction[Item, Item.type]): Task[Unit] = maybeFail
  }
}

object AbstractSplitGroupCommitSpec {
  case class Item(name: String, _id: Id[Item] = Item.id()) extends Document[Item]

  object Item extends DocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  case class Tag(name: String, _id: Id[Tag] = Tag.id()) extends Document[Tag]

  object Tag extends DocumentModel[Tag] with JsonConversion[Tag] {
    override implicit val rw: RW[Tag] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  case class Plain(name: String, _id: Id[Plain] = Plain.id()) extends Document[Plain]

  object Plain extends DocumentModel[Plain] with JsonConversion[Plain] {
    override implicit val rw: RW[Plain] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  case class Small(name: String, _id: Id[Small] = Small.id()) extends Document[Small]

  object Small extends DocumentModel[Small] with JsonConversion[Small] {
    override implicit val rw: RW[Small] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  type Split[D <: Document[D], M <: DocumentModel[D]] = SplitCollection[D, M, RocksDBStore[D, M], LuceneStore[D, M]]

  class DB(dir: Path, durability: LuceneDurability) extends LightDB {
    override type SM = SplitStoreManager[RocksDBSharedStore, LuceneStoreManager]
    override val storeManager: SM = SplitStoreManager(RocksDBSharedStore(dir.resolve("rocks")), LuceneStore.withDurability(durability))
    override def name: String = "SplitGroupCommit"
    override lazy val directory: Option[Path] = Some(dir)
    override protected def disposeOnShutdown: Boolean = false
    val items: Split[Item, Item.type] = store(Item).withGroupCommit()()
    val tags: Split[Tag, Tag.type] = store(Tag).withGroupCommit()()
    val plain: Split[Plain, Plain.type] = store(Plain)()
    val small: Split[Small, Small.type] = store(Small).withGroupCommit(50)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
