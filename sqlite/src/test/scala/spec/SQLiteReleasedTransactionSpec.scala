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
 * A transaction used after its scope released it fails at the call instead of
 * silently dropping a write or reopening a connection nothing will close.
 */
@EmbeddedTest
class SQLiteReleasedTransactionSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  case class Record(name: String, _id: Id[Record] = Record.id()) extends Document[Record]

  object Record extends DocumentModel[Record] with JsonConversion[Record] {
    override implicit val rw: RW[Record] = RW.gen
    val name: F[String] = field("name", (d: Record) => d.name)
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

  private def released: Task[DB.records.TX] = DB.records.transaction(tx => Task.pure(tx))

  "a released transaction" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "reject an insert" in {
      released.flatMap(tx => tx.insert(Record("late")).attempt).map { result =>
        result.failed.get shouldBe an[IllegalStateException]
        result.failed.get.getMessage should include("used after release")
      }
    }
    "reject a read" in {
      released.flatMap(tx => tx.get(Record.id("missing")).attempt).map { result =>
        val causes = Iterator.iterate(result.failed.get)(_.getCause).takeWhile(_ != null).toList
        causes.exists(_.isInstanceOf[IllegalStateException]) shouldBe true
        causes.map(_.getMessage).mkString(" / ") should include("used after release")
      }
    }
    "leave the store usable by a fresh transaction" in {
      DB.records.transaction(_.insert(Record("fresh", _id = Record.id("f")))).flatMap { _ =>
        DB.records.transaction(_.get(Record.id("f")))
      }.map(_.map(_.name) shouldBe Some("fresh"))
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }
}
