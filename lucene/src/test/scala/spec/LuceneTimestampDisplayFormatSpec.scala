package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.lucene.LuceneStore
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.AsyncTaskSpec

import java.nio.file.Path

/**
 * A Timestamp field is indexed as a numeric (Int) Lucene field, but `Timestamp.ToJson` is a process
 * global that a caller may switch to a date-only display format ("2026-09-08") while other documents
 * are being written. The indexer must still persist those documents - failing the write here silently
 * loses the document (a long-lived daemon lost its workflow records this way).
 */
@EmbeddedTest
class LuceneTimestampDisplayFormatSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  case class Event(name: String, at: Timestamp, _id: Id[Event] = Event.id()) extends Document[Event]

  object Event extends DocumentModel[Event] with JsonConversion[Event] {
    override implicit val rw: RW[Event] = RW.gen
    val name: I[String] = field.index(_.name)
    val at: I[Timestamp] = field.index(_.at)
  }

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = LuceneStore
    override lazy val directory: Option[Path] = Some(Path.of("db/LuceneTimestampDisplayFormatSpec"))
    val events: Collection[Event, Event.type] = store(Event)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private val when = Timestamp(1_757_286_000_000L) // 2025-09-07

  "LuceneTimestampDisplayFormatSpec" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "index a Timestamp document while the global display format is date-only" in {
      val previous = Timestamp.ToJson
      Timestamp.ToJson = Timestamp.YearMonthDayJson
      val insert = DB.events.transaction(_.insert(Event("deliver", when)))
      insert.guarantee(rapid.Task { Timestamp.ToJson = previous }).map(_ => succeed)
    }
    // The date-only display format is lossy by design (it drops the time of day), so the document must
    // survive and its Timestamp must land on the same calendar day; exact millis cannot be expected.
    "read the document back on the same calendar day" in {
      DB.events.transaction { tx =>
        tx.query.filter(_.name === "deliver").toList.map { list =>
          list.length should be(1)
          val read = list.head.at
          (read.year, read.month, read.day) should be((when.year, when.month, when.day))
        }
      }
    }
    "truncate the database" in {
      DB.events.transaction(_.truncate).succeed
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }
}
