package spec

import fabric.*
import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * A document field the model does not declare is stored in a column registered for it at
 * initialization, typed as JSON because the store has no declared field to read its type from. A
 * `String` value in such a column is written as its text, so text that is valid JSON, or a JSON
 * scalar, must still come back as the same `String`: the concrete record's definition (the
 * subtype named by the discriminator, for a polymorphic root) says the field is a string. Values
 * the record declares as JSON, numbers or booleans still decode as such.
 */
abstract class AbstractUndeclaredStringFieldSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected def storeManager: CollectionManager

  protected lazy val specName: String = getClass.getSimpleName

  private val texts: List[String] = List(
    """{"results": [{"id": "ORD-1001", "qty": 2}, {"id": "ORD-1002", "qty": 1}]}""",
    """{"results":[{"id":"ORD-1001"}]}""",
    """{"results": [{"id": "ORD-10""",
    """[1, 2, 3]""",
    "true",
    "false",
    "123",
    "-7",
    "1.0E-7",
    "null",
    "\"quoted\"",
    "plain text"
  )

  private val notes: List[Note] = texts.zipWithIndex.map { case (t, i) =>
    Note(
      summary = t,
      detail = Some(t),
      lines = List(t, "other"),
      payload = obj("kind" -> str("demo"), "n" -> num(i)),
      _id = Id[Entry](s"note-$i")
    )
  }
  private val emptyDetail: Note = Note(
    summary = "",
    detail = None,
    lines = Nil,
    payload = arr(num(1), str("two")),
    _id = Id[Entry]("note-none")
  )
  private val tally: Tally = Tally(count = 123, flag = true, label = "true", _id = Id[Entry]("tally"))
  private val memos: List[Memo] = texts.zipWithIndex.map { case (t, i) => Memo(text = t, _id = Id[Memo](s"memo-$i")) }

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "insert records whose string fields hold JSON-looking text" in {
      for {
        _ <- db.entries.transaction(_.insert(emptyDetail :: tally :: notes))
        _ <- db.memos.transaction(_.insert(memos))
      } yield succeed
    }
    "read every string of a polymorphic record back as the same text" in {
      db.entries.transaction { tx =>
        Task.sequence(notes.map(n => tx(n._id))).map { fetched =>
          fetched.zip(notes).foreach { case (got, expected) =>
            got should be(expected)
          }
          succeed
        }
      }
    }
    "read a polymorphic record's absent option, JSON payload, number and boolean back as typed" in {
      db.entries.transaction { tx =>
        for {
          n <- tx(emptyDetail._id)
          t <- tx(tally._id)
        } yield {
          n should be(emptyDetail)
          t should be(tally)
        }
      }
    }
    "stream every polymorphic record back unchanged" in {
      db.entries.transaction { tx =>
        tx.stream.toList.map { list =>
          val expected = emptyDetail :: tally :: notes
          list.filterNot(expected.contains) should be(Nil)
          list.size should be(expected.size)
        }
      }
    }
    "read every string of a plain record's undeclared field back as the same text" in {
      db.memos.transaction { tx =>
        tx.stream.toList.map { list =>
          list.toSet should be(memos.toSet)
        }
      }
    }
    "truncate the database" in {
      db.truncate().succeed
    }
    "dispose the database" in {
      db.dispose.succeed
    }
  }

  lazy val db: DB = new DB

  class DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager

    override def name: String = specName

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val entries: Collection[Entry, Entry.type] = store(Entry)()
    val memos: Collection[Memo, Memo.type] = store(Memo)()

    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}

trait Entry extends RecordDocument[Entry]

case class Note(summary: String,
                detail: Option[String],
                lines: List[String],
                payload: Json,
                _id: Id[Entry] = Entry.id(),
                created: Timestamp = Timestamp(0L),
                modified: Timestamp = Timestamp(0L)) extends Entry derives RW

case class Tally(count: Int,
                 flag: Boolean,
                 label: String,
                 _id: Id[Entry] = Entry.id(),
                 created: Timestamp = Timestamp(0L),
                 modified: Timestamp = Timestamp(0L)) extends Entry derives RW

object Entry extends PolyType[Entry]()(using scala.reflect.ClassTag(classOf[Entry]))
  with RecordDocumentModel[Entry]
  with JsonConversion[Entry] {
  register(summon[RW[Note]], summon[RW[Tally]])

  implicit override val rw: RW[Entry] = polyRW
}

case class Memo(text: String,
                _id: Id[Memo] = Memo.id(),
                created: Timestamp = Timestamp(0L),
                modified: Timestamp = Timestamp(0L)) extends RecordDocument[Memo]

object Memo extends RecordDocumentModel[Memo] with JsonConversion[Memo] {
  implicit override val rw: RW[Memo] = RW.gen
}
