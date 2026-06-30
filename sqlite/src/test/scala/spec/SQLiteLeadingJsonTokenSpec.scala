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
import rapid.AsyncTaskSpec

import java.nio.file.{Files, Path}
import java.util.Comparator

/**
 * Regression for the "leading JSON token" read bug: a [[String]] column whose value merely *looks*
 * like the start of a JSON literal (a bare number, `true`, an unbalanced `[`/`{`, or a subtitle
 * cue beginning with "1\n00:00...") used to be read back as the parsed leading token (every `.srt`
 * collapsed to "1"). The fix is two guards in `SQLStoreTransaction.toJson`: trust the field's typed
 * `RW` (a `String`/`Option[String]` field is returned verbatim, never parsed) and, for genuinely
 * untyped scalars, gate parsing on a byte-exact round-trip. This proves both, end-to-end on SQLite.
 */
@EmbeddedTest
class SQLiteLeadingJsonTokenSpec extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers {

  case class Cue(text: String,
                 note: Option[String],
                 lines: List[String],
                 _id: Id[Cue] = Cue.id()) extends Document[Cue]

  object Cue extends DocumentModel[Cue] with JsonConversion[Cue] {
    override implicit val rw: RW[Cue] = RW.gen
    val text: I[String] = field.index(_.text)
    val note: I[Option[String]] = field.index(_.note)
    val lines: I[List[String]] = field.index(_.lines)
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
    val cues: Collection[Cue, Cue.type] = store(Cue)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  // Each value is a String that begins with (or entirely is) something a JSON parser would happily
  // consume as a leading token, yet must round-trip byte-for-byte.
  private val srt = "1\n00:00:01,000 --> 00:00:04,000\nHello, world."
  private val cues = List(
    Cue(text = "1", note = Some("1"), lines = List("1", "2", "3")),
    Cue(text = srt, note = Some(srt), lines = List(srt, "2\n00:00:05,000 --> 00:00:08,000\nNext")),
    Cue(text = "true", note = Some("false"), lines = List("true", "false", "null")),
    Cue(text = "3.14 is pi", note = None, lines = List("3.14", "-7", "1e9")),
    Cue(text = "[draft]", note = Some("{wip"), lines = List("[a", "b]", "{c", "d}")),
    Cue(text = "123 Main Street", note = Some("42"), lines = List("123 Main Street", "42"))
  )

  "SQLiteLeadingJsonTokenSpec" should {
    "initialize" in {
      DB.init.succeed
    }
    "insert cues whose string values look like JSON leading tokens" in {
      DB.cues.transaction(_.insert(cues)).map(_.size should be(cues.size))
    }
    "round-trip every value byte-exact via _id lookup" in {
      DB.cues.transaction { tx =>
        rapid.Task.sequence(cues.map(c => tx(c._id))).map { fetched =>
          fetched.map(f => f.copy(_id = f._id)).toSet should be(cues.toSet)
        }
      }
    }
    "round-trip every value via a full stream/query" in {
      DB.cues.transaction { tx =>
        tx.query.toList.map { all =>
          all.toSet should be(cues.toSet)
          all.find(_._id == cues.head._id).map(_.text) should be(Some("1"))
          all.find(_._id == cues(1)._id).map(_.text) should be(Some(srt))
          all.find(_._id == cues(1)._id).map(_.lines.head) should be(Some(srt))
        }
      }
    }
    "filter by a string field whose value is a bare number" in {
      DB.cues.transaction { tx =>
        tx.query.filter(_.text === "1").toList.map(_.map(_.text) should be(List("1")))
      }
    }
    "dispose" in {
      DB.dispose.succeed
    }
  }
}
