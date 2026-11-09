package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.field.Field.Tokenized
import lightdb.filter.*
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task, taskSeq2Ops}

import java.nio.file.Path
import java.sql.{Connection, DriverManager}

/**
 * PostgreSQL's text and jsonb refuse a NUL character, and its UTF-8 encoding has no form for an unpaired UTF-16 surrogate. Every
 * string the store binds, in a text column, inside a JSON column (values and keys), in a tokenized column and in a filter, has
 * each NUL and each unpaired surrogate replaced with U+FFFD; a surrogate pair is kept.
 */
@EmbeddedTest
class PostgreSQLTextBindingSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with BeforeAndAfterAll with PostgreSQLAvailability {
  private val schema = "PostgreSQLTextBindingSpec"

  private val Nul = "a\u0000b"
  private val LoneHigh = "c\uD800d"
  private val LoneLow = "e\uDC00f"
  private val Emoji = s"g${new String(Character.toChars(0x1f600))}h"
  private val Replacement = 0xfffd.toChar

  private val cleaned = Map(
    Nul -> s"a${Replacement}b",
    LoneHigh -> s"c${Replacement}d",
    LoneLow -> s"e${Replacement}f",
    Emoji -> Emoji
  )

  private val cases = List("nul" -> Nul, "high" -> LoneHigh, "low" -> LoneLow, "emoji" -> Emoji)

  private def note(id: String, value: String): Note =
    Note(text = value, detail = Detail(value, Map(value -> value)), body = s"word $value word", tags = List(value), _id = Id(id))

  private def withJdbc[T](f: Connection => T): T = {
    val c = DriverManager.getConnection(PostgreSQLTestSupport.jdbcUrl.get, PostgreSQLTestSupport.username, PostgreSQLTestSupport.password)
    try f(c) finally c.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if PostgreSQLTestSupport.jdbcUrl.isDefined then withJdbc { c =>
      val st = c.createStatement()
      try st.executeUpdate(s"""DROP SCHEMA IF EXISTS "$schema" CASCADE""")
      finally st.close()
    }
  }

  private def ids(filter: Note.type => Filter[Note]): Task[Set[String]] =
    DB.notes.transaction(_.query.filter(filter).toList).map(_.map(_._id.value).toSet)

  "Text bound by the PostgreSQL store" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "write records whose text holds a NUL, lone surrogates and an emoji, in one batch" in {
      DB.notes.transaction(_.upsert(cases.map((id, value) => note(id, value)))).succeed
    }
    "write a record on its own" in {
      DB.notes.transaction(_.upsert(note("single", Nul))).succeed
    }
    "read each back with U+FFFD for what was invalid and the emoji intact" in {
      DB.notes.transaction { tx =>
        cases.map((id, _) => tx(Id[Note](id))).tasks
      }.map { notes =>
        notes.zip(cases).foreach { case (n, (_, value)) =>
          val expected = cleaned(value)
          n.text should be(expected)
          n.detail should be(Detail(expected, Map(expected -> expected)))
          n.body should be(s"word $expected word")
          n.tags should be(List(expected))
        }
        succeed
      }
    }
    "find each by its original text, in a text, list and tokenized filter" in {
      cases.map { (id, value) =>
        for {
          byText <- ids(_.text === value)
          byTag <- ids(_.tags has value)
          byBody <- ids(_.body === s"word $value word")
        } yield {
          byText should contain(id)
          byTag should contain(id)
          byBody should contain(id)
        }
      }.tasks.map(_ => succeed)
    }
    "dispose" in {
      DB.notes.transaction(_.truncate).flatMap(_ => DB.dispose).succeed
    }
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = PostgreSQLTestSupport.storeManager
    override def name: String = schema
    lazy val directory: Option[Path] = None
    val notes: Collection[Note, Note.type] = store(Note).withName("Note")()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Detail(label: String, attributes: Map[String, String]) derives RW

  case class Note(text: String,
                  detail: Detail,
                  body: String,
                  tags: List[String],
                  created: Timestamp = Timestamp(),
                  modified: Timestamp = Timestamp(),
                  _id: Id[Note] = Note.id()) extends RecordDocument[Note]

  object Note extends RecordDocumentModel[Note] with JsonConversion[Note] {
    override implicit val rw: RW[Note] = RW.gen
    val text: I[String] = field.index("text", (n: Note) => n.text)
    val detail: F[Detail] = field("detail", (n: Note) => n.detail)
    val body: Tokenized[Note] = field.tokenized("body", (n: Note) => n.body)
    val tags: I[List[String]] = field.index("tags", (n: Note) => n.tags)
  }
}
