package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.error.{JoinRefusedException, TransactionBoundExceededException}
import lightdb.id.Id
import lightdb.postgresql.{PostgreSQLStore, PostgreSQLStoreManager}
import lightdb.sql.connect.HikariConnectionManager
import lightdb.sql.query.SQLQuery
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task, taskSeq2Ops}

import java.nio.file.Path
import java.sql.{Connection, DriverManager}

/**
 * A transaction joined to another on PostgreSQL borrows the owner's connection: its writes are invisible on another
 * connection until the owner commits, and roll back with the owner; a failing flush of its writes aborts the owner; a
 * failed read on it aborts the owner and never replaces the owner's connection; a join across connection managers is
 * refused; and a group-commit owner fails loud past its bound and writes nothing.
 */
@EmbeddedTest
class PostgreSQLJoinSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll with PostgreSQLAvailability {
  import PostgreSQLJoinSpec.*

  private val schema = "PostgreSQLJoinSpec"

  class DB(manager: PostgreSQLStoreManager) extends LightDB {
    override type SM = PostgreSQLStoreManager
    override val storeManager: PostgreSQLStoreManager = manager
    override def name: String = schema
    lazy val directory: Option[Path] = Some(Path.of(s"db/$schema"))
    override protected def disposeOnShutdown: Boolean = false
    val items: PostgreSQLStore[Item, Item.type] = store(Item)()
    val notes: PostgreSQLStore[Note, Note.type] = store(Note)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private def withJdbc[T](f: Connection => T): T = {
    val c = DriverManager.getConnection(PostgreSQLTestSupport.jdbcUrl.get, PostgreSQLTestSupport.username, PostgreSQLTestSupport.password)
    try f(c) finally c.close()
  }

  /** Whether another connection sees the note row, outside any LightDB transaction. */
  private def visibleElsewhere(id: String): Boolean = withJdbc { c =>
    val st = c.createStatement()
    try {
      val rs = st.executeQuery(s"""SELECT COUNT(*) FROM "$schema"."Note" WHERE "_id" = '$id'""")
      rs.next()
      rs.getInt(1) > 0
    } finally st.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if PostgreSQLTestSupport.jdbcUrl.isDefined then withJdbc { c =>
      val st = c.createStatement()
      try st.executeUpdate(s"""DROP SCHEMA IF EXISTS "$schema" CASCADE""")
      finally st.close()
    }
  }

  private lazy val db = new DB(PostgreSQLTestSupport.storeManager)

  private def item(id: String) = db.items.transaction(_.get(Id[Item](id)))
  private def note(id: String) = db.notes.transaction(_.get(Id[Note](id)))

  "Joined transactions on PostgreSQL" should {
    "initialize" in db.init.map(_ => db.notes.supportsJoin shouldBe true)
    "keep a joined write invisible on another connection until the owner commits" in {
      for
        whileOpen <- db.items.transaction { owner =>
          owner.insert(Item("owner", _id = Id("i1")))
            .next(db.notes.transaction.joining(owner)(_.insert(Note("joined", _id = Id("n1")))))
            .map(_ => visibleElsewhere("n1"))
        }
        afterCommit = visibleElsewhere("n1")
        i <- item("i1")
        n <- note("n1")
      yield {
        whileOpen shouldBe false
        afterCommit shouldBe true
        (i.map(_.name), n.map(_.text)) shouldBe (Some("owner"), Some("joined"))
      }
    }
    "roll a joined write back with its owner" in {
      for
        failed <- db.items.transaction { owner =>
          owner.insert(Item("owner", _id = Id("i2")))
            .next(db.notes.transaction.joining(owner)(_.insert(Note("joined", _id = Id("n2")))))
            .next(Task.error[Unit](new RuntimeException("owner fails")))
        }.attempt
        i <- item("i2")
        n <- note("n2")
      yield {
        failed.isFailure shouldBe true
        (i, n) shouldBe (None, None)
        visibleElsewhere("n2") shouldBe false
      }
    }
    "abort the owner when the joined transaction's flush fails" in {
      for
        _ <- db.notes.transaction(_.insert(Note("existing", _id = Id("n3"))))
        failed <- db.items.transaction { owner =>
          owner.insert(Item("owner", _id = Id("i3")))
            .next(db.notes.transaction.joining(owner)(_.insert(Note("duplicate", _id = Id("n3")))).attempt)
            .unit
        }.attempt
        i <- item("i3")
        n <- note("n3")
      yield {
        failed.isFailure shouldBe true
        i shouldBe None
        n.map(_.text) shouldBe Some("existing")
      }
    }
    "abort the owner on a failed read in the joined transaction, keeping the owner's connection" in {
      var readFailed = false
      var keptConnection = false
      for
        result <- db.items.transaction { owner =>
          val before = owner.state.openConnection()
          owner.insert(Item("owner", _id = Id("i4")))
            .next(owner.flush)
            .next(db.notes.transaction.joining(owner) { joined =>
              Task(joined.resultsFor(SQLQuery.parse(s"""SELECT * FROM "$schema"."NoSuchTable""""))).attempt.map { r =>
                readFailed = r.isFailure
              }
            }.attempt)
            .map(_ => keptConnection = owner.state.openConnection() eq before)
        }.attempt
        i <- item("i4")
      yield {
        readFailed shouldBe true
        keptConnection shouldBe true
        result.isFailure shouldBe true
        i shouldBe None
      }
    }
    "refuse a join across connection managers" in {
      val other = new DB(PostgreSQLStoreManager(HikariConnectionManager(PostgreSQLTestSupport.sqlConfig)))
      for
        _ <- other.init
        refused <- db.items.transaction { owner =>
          other.notes.transaction.joining(owner)(_.insert(Note("never", _id = Id("n5")))).unit
        }.attempt
        _ <- other.dispose
      yield refused.failed.get shouldBe a[JoinRefusedException]
    }
    "fail loud past a group-commit bound and write nothing" in {
      for
        failed <- db.items.transaction.withGroupCommit(100) { owner =>
          (1 to 60).toList.map(i => owner.insert(Item(s"bulk$i", _id = Id(s"bulk$i")))).tasks
            .next(db.notes.transaction.joining(owner)(joined => (1 to 41).toList.map(i => joined.insert(Note(s"bulk$i", _id = Id(s"nbulk$i")))).tasks))
            .unit
        }.attempt
        i <- item("bulk1")
        n <- note("nbulk1")
      yield {
        failed.failed.get shouldBe a[TransactionBoundExceededException]
        (i, n) shouldBe (None, None)
      }
    }
    "dispose" in db.dispose.succeed
  }
}

object PostgreSQLJoinSpec {
  case class Item(name: String,
                  created: Timestamp = Timestamp(),
                  modified: Timestamp = Timestamp(),
                  _id: Id[Item] = Item.id()) extends RecordDocument[Item]

  object Item extends RecordDocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  case class Note(text: String,
                  created: Timestamp = Timestamp(),
                  modified: Timestamp = Timestamp(),
                  _id: Id[Note] = Note.id()) extends RecordDocument[Note]

  object Note extends RecordDocumentModel[Note] with JsonConversion[Note] {
    override implicit val rw: RW[Note] = RW.gen
    val text: I[String] = field.index(_.text)
  }
}
