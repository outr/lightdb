package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.sql.SQLiteStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * Regression: a `get` must observe writes made earlier in the SAME transaction
 * ("read-your-writes").
 *
 * SQL stores batch writes through `QueuedWriteHandler`, which queues every op
 * until commit/flush. If `QueuedWriteHandler.get` doesn't consult that queue, a
 * get for a still-queued id falls through to the backend — which hasn't received
 * the row yet — and wrongly reports a miss. That silently breaks any
 * get-modify-upsert within one transaction (e.g. event-sourced delta application:
 * insert an event, then read it back to apply a state delta in the same
 * transaction — the read returns None and the modify no-ops).
 *
 * The embedded stores were unaffected; only SQL backends queue writes this way.
 */
@EmbeddedTest
class SQLiteReadYourWritesSpec extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers {

  case class Record(name: String, state: String, _id: Id[Record] = Record.id()) extends Document[Record]

  object Record extends DocumentModel[Record] with JsonConversion[Record] {
    override implicit val rw: RW[Record] = RW.gen
    val name: F[String] = field("name", (d: Record) => d.name)
    val state: F[String] = field("state", (d: Record) => d.state)
  }

  private val specName = getClass.getSimpleName
  private val dbPath: Path = Path.of(s"db/$specName")
  if Files.exists(dbPath) then {
    Files.walk(dbPath).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = SQLiteStore
    override def name: String = specName
    override lazy val directory: Option[Path] = Some(dbPath)
    val records: Collection[Record, Record.type] = store(Record)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  "SQLiteReadYourWritesSpec" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "observe an insert made earlier in the same transaction" in {
      DB.records.transaction { tx =>
        for {
          _ <- tx.insert(Record("alpha", "Active", _id = Record.id("a")))
          found <- tx.get(Record.id("a"))
        } yield found.map(_.name) should be(Some("alpha"))
      }
    }
    "apply a get-modify-upsert within one transaction (the event-delta pattern)" in {
      DB.records.transaction { tx =>
        for {
          _ <- tx.insert(Record("beta", "Active", _id = Record.id("b")))
          current <- tx.get(Record.id("b"))
          _ <- current match {
            case Some(r) => tx.upsert(r.copy(state = "Complete"))
            case None    => Task.unit
          }
          finalState <- tx.get(Record.id("b"))
        } yield finalState.map(_.state) should be(Some("Complete"))
      }
    }
    "persist the delta-applied state after the transaction commits" in {
      DB.records.transaction { tx =>
        tx.get(Record.id("b")).map(_.map(_.state) should be(Some("Complete")))
      }
    }
    "observe an upsert overwrite made earlier in the same transaction" in {
      DB.records.transaction { tx =>
        for {
          _ <- tx.upsert(Record("gamma-v1", "Active", _id = Record.id("c")))
          _ <- tx.upsert(Record("gamma-v2", "Complete", _id = Record.id("c")))
          found <- tx.get(Record.id("c"))
        } yield found.map(r => r.name -> r.state) should be(Some("gamma-v2" -> "Complete"))
      }
    }
    "observe a delete made earlier in the same transaction" in {
      DB.records.transaction { tx =>
        for {
          _ <- tx.insert(Record("doomed", "Active", _id = Record.id("d")))
          _ <- tx.delete(Record.id("d"))
          found <- tx.get(Record.id("d"))
        } yield found should be(None)
      }
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }
}
