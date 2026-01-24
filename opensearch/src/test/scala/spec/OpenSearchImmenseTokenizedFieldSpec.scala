package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchImmenseTokenizedFieldSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class HugeTextDoc(fullText: String, _id: Id[HugeTextDoc] = HugeTextDoc.id()) extends Document[HugeTextDoc]
  object HugeTextDoc extends DocumentModel[HugeTextDoc] with JsonConversion[HugeTextDoc] {
    override implicit val rw: RW[HugeTextDoc] = RW.gen
    val fullText: T = field.tokenized("fullText", _.fullText)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = OpenSearchImmenseTokenizedFieldSpec.this.getClass.getSimpleName.replace("$", "")
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: OpenSearchStore[HugeTextDoc, HugeTextDoc.type] = store(HugeTextDoc)
  }

  "OpenSearch immense tokenized field" should {
    "not fail indexing when tokenized text exceeds Lucene's keyword term limit (no fullText.keyword subfield)" in {
      val db = new DB

      val huge = "x" * 200_000
      val doc = HugeTextDoc(fullText = huge, _id = Id("huge"))

      val test = for
        _ <- db.init
        _ <- db.docs.transaction { tx =>
          tx.truncate.next(tx.insert(doc)).next(tx.commit)
        }
        found <- db.docs.transaction { tx =>
          tx.get(Id[HugeTextDoc]("huge"))
        }
        _ <- db.dispose
      yield {
        found.nonEmpty shouldBe true
      }

      test
    }
  }
}

