package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.cache.CacheConfig
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.sql.SQLiteStore
import lightdb.store.CollectionManager
import lightdb.transaction.Transaction
import lightdb.transaction.batch.BatchConfig
import lightdb.trigger.StoreTrigger
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import rapid.Task
import scala.concurrent.duration.*

@EmbeddedTest
class SQLiteTransactionFailureSpec extends AnyWordSpec with Matchers {
  case class Row(value: String, _id: Id[Row] = Id[Row]("row")) extends Document[Row]
  object Row extends DocumentModel[Row] with JsonConversion[Row] {
    implicit val rw: RW[Row] = RW.gen
    val value = field.index(_.value)
  }
  class Fixture extends LightDB {
    type SM = CollectionManager
    val storeManager: CollectionManager = SQLiteStore
    val directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil
    val rows = store(Row).withCache(CacheConfig.lru(10))()
    val other = store(Row).withName("other")()
  }
  def fixture(f: Fixture => Unit): Unit = {
    val db = new Fixture
    db.init.sync()
    try f(db) finally db.dispose.sync()
  }
  val modes = List(BatchConfig.Direct, BatchConfig.StoreNative, BatchConfig.Buffered(10),
    BatchConfig.Queued(10), BatchConfig.Async(1, 1, 1.millis, 10))
  "Transaction failure" should {
    modes.foreach { mode =>
      s"rollback body failures without stale cache or commit callbacks with $mode" in fixture { db =>
        db.rows.transaction(_.upsert(Row("old"))).sync()
        var committed = 0
        var aborted = 0
        db.rows.onCommittedChange(() => Task { committed += 1 })
        db.rows.trigger += new StoreTrigger[Row, Row.type] {
          override def transactionRolledBack(tx: Transaction[Row, Row.type]): Task[Unit] = Task { aborted += 1 }
        }
        val boom = new IllegalStateException("injected body error")
        val thrown = intercept[IllegalStateException] {
          db.rows.transaction.withBatch(mode) { tx =>
            tx.upsert(Row("bad")).next(tx.flush).next(Task.error(boom))
          }.sync()
        }
        thrown shouldBe boom
        committed shouldBe 0
        aborted shouldBe 1
        db.rows.transaction(_.get(Id[Row]("row"))).sync().map(_.value) shouldBe Some("old")
        db.rows.cache.foreach(_.clear())
        db.rows.transaction(_.get(Id[Row]("row"))).sync().map(_.value) shouldBe Some("old")
        db.rows.transaction.active shouldBe 0
        db.rows.transaction(_.upsert(Row("next"))).sync()
        committed shouldBe 1
      }
    }
    "release a transaction when constructing its body throws synchronously" in fixture { db =>
      intercept[IllegalArgumentException] {
        db.rows.transaction(_ => throw new IllegalArgumentException("construction")).sync()
      }
      db.rows.transaction.active shouldBe 0
      db.rows.transaction(_.upsert(Row("usable"))).sync()
    }
    "discard buffered writes on explicit rollback and never recommit at release" in fixture { db =>
      db.rows.transaction.withBufferedBatch() { tx => tx.insert(Row("discard")).next(tx.rollback) }.sync()
      db.rows.transaction(_.query.toList).sync() shouldBe Nil
      db.rows.transaction.active shouldBe 0
    }
    "rollback and close after a transaction-end hook fails" in fixture { db =>
      val hook = new StoreTrigger[Row, Row.type] {
        override def transactionEnd(tx: Transaction[Row, Row.type]): Task[Unit] = Task.error(new IllegalStateException("end hook"))
      }
      db.rows.trigger += hook
      intercept[IllegalStateException](db.rows.transaction(_.upsert(Row("bad"))).sync())
      db.rows.trigger -= hook
      db.rows.transaction.active shouldBe 0
      db.rows.transaction(_.query.toList).sync() shouldBe Nil
    }
    "release resources when a start hook fails" in fixture { db =>
      val hook = new StoreTrigger[Row, Row.type] {
        override def transactionStart(tx: Transaction[Row, Row.type]): Task[Unit] = Task.error(new IllegalStateException("start hook"))
      }
      db.rows.trigger += hook
      intercept[IllegalStateException](db.rows.transaction(_.upsert(Row("bad"))).sync())
      db.rows.trigger -= hook
      db.rows.transaction.active shouldBe 0
      db.rows.transaction(_.query.toList).sync() shouldBe Nil
    }
    "abort both stores when a multi-store body fails" in fixture { db =>
      val manager = new lightdb.transaction.TransactionManager
      intercept[IllegalStateException] {
        manager(db.rows, db.other) { (a, b) =>
          a.upsert(Row("a")).next(b.upsert(Row("b"))).next(Task.error(new IllegalStateException("body")))
        }.sync()
      }
      db.rows.transaction.active shouldBe 0
      db.other.transaction.active shouldBe 0
      db.rows.transaction(_.query.toList).sync() shouldBe Nil
      db.other.transaction(_.query.toList).sync() shouldBe Nil
    }
    "abort the first store if acquiring the next store fails" in fixture { db =>
      val hook = new StoreTrigger[Row, Row.type] {
        override def transactionStart(tx: Transaction[Row, Row.type]): Task[Unit] = Task.error(new IllegalStateException("acquire"))
      }
      db.other.trigger += hook
      intercept[IllegalStateException] {
        new lightdb.transaction.TransactionManager().apply(db.rows, db.other)((_, _) => Task.unit).sync()
      }
      db.other.trigger -= hook
      db.rows.transaction.active shouldBe 0
      db.other.transaction.active shouldBe 0
    }
    "discard a failed shared transaction rather than committing it on idle expiry" in fixture { db =>
      intercept[IllegalStateException] {
        db.rows.transaction.shared("failure", 20.millis) { tx =>
          tx.upsert(Row("bad")).next(Task.error(new IllegalStateException("shared")))
        }.sync()
      }
      Task.sleep(60.millis).sync()
      db.rows.transaction(_.query.toList).sync() shouldBe Nil
      db.rows.transaction.shared("failure", 20.millis)(_.upsert(Row("good"))).sync()
      Task.sleep(60.millis).sync()
      db.rows.transaction(_.query.toList).sync().map(_.value) shouldBe List("good")
    }
    "allow a fresh shared acquisition after initialization fails" in fixture { db =>
      val hook = new StoreTrigger[Row, Row.type] {
        override def transactionStart(tx: Transaction[Row, Row.type]): Task[Unit] =
          Task.error(new IllegalStateException("shared acquisition"))
      }
      db.rows.trigger += hook
      intercept[IllegalStateException](db.rows.transaction.shared("init-failure", 20.millis)(_ => Task.unit).sync())
      db.rows.trigger -= hook
      db.rows.transaction.active shouldBe 0
      db.rows.transaction.shared("init-failure", 20.millis)(_.upsert(Row("good"))).sync()
      Task.sleep(60.millis).sync()
      db.rows.transaction(_.query.toList).sync().map(_.value) shouldBe List("good")
    }
    "abort every acquired shard when a MultiStore body fails" in fixture { db =>
      val stores = new lightdb.store.multi.MultiStore(Map("a" -> db.rows, "b" -> db.other))
      intercept[IllegalStateException] {
        stores.transaction(tx => tx("a").upsert(Row("a")).next(tx("b").upsert(Row("b")))
          .next(Task.error(new IllegalStateException("sharded body")))).sync()
      }
      db.rows.transaction.active shouldBe 0
      db.other.transaction.active shouldBe 0
      db.rows.transaction(_.query.toList).sync() shouldBe Nil
      db.other.transaction(_.query.toList).sync() shouldBe Nil
    }
    "preserve a body failure and report cleanup errors as suppressed" in fixture { db =>
      val hook = new StoreTrigger[Row, Row.type] {
        override def transactionRolledBack(tx: Transaction[Row, Row.type]): Task[Unit] =
          Task.error(new IllegalArgumentException("rollback observer"))
      }
      db.rows.trigger += hook
      val boom = new IllegalStateException("body")
      val thrown = intercept[IllegalStateException] {
        db.rows.transaction(_ => Task.error(boom)).sync()
      }
      db.rows.trigger -= hook
      thrown shouldBe boom
      thrown.getSuppressed.map(_.getMessage) should contain("rollback observer")
      db.rows.transaction.active shouldBe 0
    }
    "close before notifying all commit observers even when one throws synchronously" in fixture { db =>
      var notified = false
      db.rows.onCommittedChange(() => Task { notified = true })
      val hook = new StoreTrigger[Row, Row.type] {
        override def transactionCommitted(tx: Transaction[Row, Row.type]): Task[Unit] =
          throw new IllegalArgumentException("committed observer")
      }
      db.rows.trigger += hook
      intercept[IllegalArgumentException](db.rows.transaction(_.upsert(Row("durable"))).sync())
      db.rows.trigger -= hook
      db.rows.transaction.active shouldBe 0
      notified shouldBe true
      db.rows.transaction(_.query.toList).sync().map(_.value) shouldBe List("durable")
    }
  }
}
