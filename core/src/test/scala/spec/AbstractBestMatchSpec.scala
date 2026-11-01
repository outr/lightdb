package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.filter.*
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

/**
 * `Sort.BestMatch` over a tokenized field ranks by relevance as BM25 does: a rare term weighs more than a common one, a term found more
 * often weighs more, and a shorter text weighs more for the same matches. A token matches whole words only.
 */
trait AbstractBestMatchSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val Common = 100

  private val notes: List[Note] =
    (1 to Common).toList.map(i => Note(s"apple pie $i", kind = if i % 2 == 0 then "a" else "b", _id = Id(s"apple$i"))) ++ List(
      Note("thread pool sizing", _id = Id("thread")),
      Note("thread files", _id = Id("threadFiles")),
      Note("already done", _id = Id("already")),
      Note("please read the file", _id = Id("read")),
      Note(s"mango with ${(1 to 20).map(i => s"word$i").mkString(" ")}", _id = Id("mangoLong")),
      Note("mango tart", _id = Id("mangoTart")),
      Note("mango mango mango", _id = Id("mango3")),
      Note("no text term here", kind = "z", _id = Id("kindOnly")),
      // the rare term, in a long body, written last so that no insertion order puts it first
      Note(s"zebra ${(1 to 20).map(i => s"filler$i").mkString(" ")}", kind = "a", _id = Id("zebra"))
    )

  private def ranked(filter: Note.type => Filter[Note], limit: Int): Task[List[(String, Double)]] =
    DB.notes.transaction(_.query.filter(filter).sort(Sort.BestMatch()).limit(limit).scored.search.flatMap(_.listWithScore))
      .map(_.map((n, s) => n._id.value -> s))

  private def ids(filter: Note.type => Filter[Note]): Task[Set[String]] =
    DB.notes.transaction(_.query.filter(filter).toList).map(_.map(_._id.value).toSet)

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert the notes" in {
      DB.notes.transaction(_.insert(notes)).succeed
    }
    "rank a rare term in a long body above many short matches of a common term, under a limit" in {
      ranked(n => n.text.exactly("zebra") || n.text.exactly("apple"), 5).map { hits =>
        hits.size should be(5)
        hits.head._1 should be("zebra")
        hits.head._2 should be > hits(1)._2
        hits.map(_._2) should be(hits.map(_._2).sorted.reverse)
      }
    }
    "rank the same way inside a conjunction with a field filter" in {
      ranked(n => (n.text.exactly("zebra") || n.text.exactly("apple")) && n.kind === "a", 3).map { hits =>
        hits.head._1 should be("zebra")
      }
    }
    "keep a row that matched on another clause" in {
      ranked(n => n.text.exactly("zebra") || n.kind === "z", 5).map(_.map(_._1).toSet should be(Set("zebra", "kindOnly")))
    }
    "rank more occurrences, then a shorter text, higher for the same term" in {
      ranked(n => n.text.exactly("mango"), 3).map(_.map(_._1) should be(List("mango3", "mangoTart", "mangoLong")))
    }
    "match a token as a whole word, not inside a longer one" in {
      for {
        read <- ids(_.text.exactly("read"))
        thread <- ids(_.text.exactly("thread"))
      } yield {
        read should be(Set("read"))
        thread should be(Set("thread", "threadFiles"))
      }
    }
    "match every token of an equality as a whole word" in {
      ids(_.text === "read file").map(_ should be(Set("read")))
    }
    "truncate the database" in {
      DB.truncate().succeed
    }
    "dispose the database" in {
      DB.dispose.succeed
    }
  }

  def storeManager: CollectionManager

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager

    override def name: String = specName

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val notes: Collection[Note, Note.type] = store(Note)()

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Note(body: String,
                  kind: String = "b",
                  created: Timestamp = Timestamp(),
                  modified: Timestamp = Timestamp(),
                  _id: Id[Note] = Note.id()) extends RecordDocument[Note]

  object Note extends RecordDocumentModel[Note] with JsonConversion[Note] {
    override implicit val rw: RW[Note] = RW.gen

    val text: T = field.tokenized("text", (d: Note) => d.body)
    val kind: I[String] = field.index("kind", (d: Note) => d.kind)
  }
}
