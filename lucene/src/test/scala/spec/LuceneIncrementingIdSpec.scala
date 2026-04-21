package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, IncrementingIdSupport, JsonConversion}
import lightdb.id.{Id, IncrementingId}
import lightdb.lucene.LuceneStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path}

@EmbeddedTest
class LuceneIncrementingIdSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  // Remove any directory state from previous test runs so `truncateOnInit` timing and the crash-recovery
  // probe work deterministically.
  private def rmTree(p: Path): Unit = if (Files.exists(p)) {
    if (Files.isDirectory(p)) Files.list(p).forEach(rmTree)
    Files.deleteIfExists(p)
  }
  rmTree(Path.of("db/LuceneIncrementingIdSpec"))
  rmTree(Path.of("db/LuceneIncrementingIdSpec-crash"))

  case class Widget(name: String, _id: Id[Widget] = Widget.id()) extends Document[Widget]

  object Widget extends DocumentModel[Widget] with IncrementingIdSupport[Widget] with JsonConversion[Widget] {
    override implicit val rw: RW[Widget] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = LuceneStore
    override lazy val directory: Option[Path] = Some(Path.of("db/LuceneIncrementingIdSpec"))
    val widgets: Collection[Widget, Widget.type] = store(Widget)()
    override protected def truncateOnInit: Boolean = true
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  "LuceneIncrementingIdSpec" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "allocate ids starting at 1" in {
      val id1 = Widget.id()
      val id2 = Widget.id()
      val id3 = Widget.id()
      
      id1 should be(IncrementingId[Widget](1L))
      id2 should be(IncrementingId[Widget](2L))
      id3 should be(IncrementingId[Widget](3L))

      id1.value should be("0000000000000000001")
      id2.value should be("0000000000000000002")
      id3.value should be("0000000000000000003")
    }
    "insert documents using allocated ids" in {
      DB.widgets.transaction { tx =>
        for {
          w1 = Widget("four")
          w2 = Widget("five")
          w3 = Widget("six")
          _  <- tx.insert(w1)
          _  <- tx.insert(w2)
          _  <- tx.insert(w3)
          count <- tx.count
        } yield {
          w1._id should be(IncrementingId[Widget](4L))
          w2._id should be(IncrementingId[Widget](5L))
          w3._id should be(IncrementingId[Widget](6L))
          count should be(3)
        }
      }
    }
    "retrieve documents by their allocated incrementing id" in {
      DB.widgets.transaction { tx =>
        tx.get(IncrementingId[Widget](4L)).map(doc => doc.map(_.name) should be(Some("four")))
      }
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }

  // --- Crash-recovery scenario ---------------------------------------------------------------------------

  case class Gadget(name: String, _id: Id[Gadget] = Gadget.id()) extends Document[Gadget]

  object Gadget extends DocumentModel[Gadget] with IncrementingIdSupport[Gadget] with JsonConversion[Gadget] {
    override implicit val rw: RW[Gadget] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  object CrashDB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = LuceneStore
    override lazy val directory: Option[Path] = Some(Path.of("db/LuceneIncrementingIdSpec-crash"))
    val gadgets: Collection[Gadget, Gadget.type] = store(Gadget)()
    override protected def truncateOnInit: Boolean = true
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  "LuceneIncrementingIdSpec crash recovery" should {
    "initialize a fresh database and insert gadgets 1..5 via the allocator" in {
      CrashDB.init.flatMap { _ =>
        CrashDB.gadgets.transaction { tx =>
          (1 to 5).foldLeft(Task.pure(())) { (acc, i) =>
            acc.map(_ => Gadget(s"g$i")).flatMap(tx.insert).map(_ => ())
          }
        }
      }.succeed
    }
    "rewind the persisted counter to 2 (simulating a crash before the debounced flush)" in {
      CrashDB.stored[Long]("_incrementingId_Gadget", 0L).set(2L).map(_ => succeed)
    }
    "dispose to simulate the crash" in {
      CrashDB.dispose.succeed
    }
    "reinitialize — next allocation should be 6, not 3" in {
      object CrashDB2 extends LightDB {
        override type SM = CollectionManager
        override val storeManager: CollectionManager = LuceneStore
        override lazy val directory: Option[Path] = Some(Path.of("db/LuceneIncrementingIdSpec-crash"))
        override protected def truncateOnInit: Boolean = false
        val gadgets: Collection[Gadget, Gadget.type] = store(Gadget)()
        override def upgrades: List[DatabaseUpgrade] = Nil
      }
      for {
        _ <- CrashDB2.init
        next <- Gadget.newId
        _ <- CrashDB2.dispose
      } yield next should be(IncrementingId[Gadget](6L))
    }
  }
}
