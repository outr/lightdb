package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort, SortDirection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchIdSortSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class IdSortDoc(name: String, _id: Id[IdSortDoc] = IdSortDoc.id()) extends Document[IdSortDoc]
  object IdSortDoc extends DocumentModel[IdSortDoc] with JsonConversion[IdSortDoc] {
    override implicit val rw: RW[IdSortDoc] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: OpenSearchStore[IdSortDoc, IdSortDoc.type] = store(IdSortDoc)
  }

  "OpenSearch sort-by-_id" should {
    "support Sort.ByField(Model._id) without requiring an `_id.keyword` mapping" in {
      val db = new DB
      val docs = List(
        IdSortDoc("a", Id("a")),
        IdSortDoc("b", Id("b")),
        IdSortDoc("c", Id("c"))
      )

      val test = for
        _ <- db.init
        _ <- db.docs.transaction { tx =>
          tx.truncate.next(tx.insert(docs)).next(tx.commit)
        }
        result <- db.docs.transaction { tx =>
          tx.query
            .sort(Sort.ByField(IdSortDoc._id, SortDirection.Ascending))
            .docs
            .stream
            .map(_._id.value)
            .toList
        }
        _ <- db.dispose
      yield {
        result shouldBe List("a", "b", "c")
      }

      test
    }
  }
}

