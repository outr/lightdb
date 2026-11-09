package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.postgresql.PostgreSQLStore
import lightdb.sql.RecordWriteException
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.postgresql.util.{PSQLException, ServerErrorMessage}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path
import java.sql.{BatchUpdateException, Connection, DriverManager}

/**
 * A batched write that PostgreSQL refuses fails with a [[RecordWriteException]] naming the store, the record the batch failed on
 * and, when the server says, its column and parameter, with the driver's exception as its cause.
 */
@EmbeddedTest
class PostgreSQLBatchFailureSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll with PostgreSQLAvailability {
  private val schema = "PostgreSQLBatchFailureSpec"
  private val table = "Entry"

  private def withJdbc[T](f: Connection => T): T = {
    val c = DriverManager.getConnection(PostgreSQLTestSupport.jdbcUrl.get, PostgreSQLTestSupport.username, PostgreSQLTestSupport.password)
    try f(c) finally c.close()
  }

  private def execute(sql: String): Unit = withJdbc { c =>
    val st = c.createStatement()
    try st.executeUpdate(sql)
    finally st.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if PostgreSQLTestSupport.jdbcUrl.isDefined then execute(s"""DROP SCHEMA IF EXISTS "$schema" CASCADE""")
  }

  private def failure(entries: List[Entry]) =
    DB.entries.transaction(_.upsert(entries)).attempt.map { result =>
      result.failed.get match {
        case e: RecordWriteException => e
        case other => fail(s"expected a RecordWriteException, got $other")
      }
    }

  "A failed batch write on PostgreSQL" should {
    "initialize the database" in {
      DB.init.map { _ =>
        execute(s"""ALTER TABLE "$schema"."$table" ADD CONSTRAINT "${table}_name_short" CHECK (length("name") <= 5)""")
        succeed
      }
    }
    "name the record a constraint refused, not the statement" in {
      failure(List(Entry("one", Some("a"), _id = Id("first")), Entry("too long", Some("b"), _id = Id("second")), Entry("three", Some("c"), _id = Id("third")))).map { e =>
        e.storeName should be(table)
        e.id should be(Some("second"))
        e.getMessage should include("'second'")
        e.getMessage should include(s"'$table'")
        e.getMessage should include(s"${table}_name_short")
        e.getMessage should not include "MERGE INTO"
        e.getCause shouldBe a[BatchUpdateException]
      }
    }
    "name the column a NOT NULL refused" in {
      execute(s"""ALTER TABLE "$schema"."$table" ALTER COLUMN "label" SET NOT NULL""")
      failure(List(Entry("one", Some("a"), _id = Id("first")), Entry("two", None, _id = Id("unlabelled")))).map { e =>
        e.id should be(Some("unlabelled"))
        e.column should be(Some("label"))
        e.getMessage should include("\"label\"")
      }
    }
    "name the column of the parameter the server refused" in {
      val server = new PSQLException(new ServerErrorMessage(
        "SERROR\u0000C22021\u0000Minvalid byte sequence for encoding \"UTF8\": 0x00\u0000Wunnamed portal parameter $5\u0000"
      ))
      val batch = new BatchUpdateException("Batch entry 1 MERGE INTO ... was aborted", "22021", 0, Array(-3, -3), server)
      batch.setNextException(server)
      val store = DB.entries.asInstanceOf[PostgreSQLStore[Entry, Entry.type]]
      val e = store.recordWriteException(batch, List("first", "second"), store.fields.map(_.name))
      e.id should be(Some("second"))
      e.parameter should be(Some(5))
      e.column should be(Some(store.fields(4).name))
      e.getMessage should include("invalid byte sequence")
      e.getMessage should include("$5")
      e.getCause should be(batch)
    }
    "dispose" in {
      DB.dispose.succeed
    }
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
    override def name: String = schema
    lazy val directory: Option[Path] = None
    val entries: Collection[Entry, Entry.type] = store(Entry).withName(table)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Entry(name: String,
                   label: Option[String],
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Entry] = Entry.id()) extends RecordDocument[Entry]

  object Entry extends RecordDocumentModel[Entry] with JsonConversion[Entry] {
    override implicit val rw: RW[Entry] = RW.gen
    val name: F[String] = field("name", (e: Entry) => e.name)
    val label: F[Option[String]] = field("label", (e: Entry) => e.label)
  }
}
