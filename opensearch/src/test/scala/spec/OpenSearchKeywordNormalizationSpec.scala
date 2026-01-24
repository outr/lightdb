package spec

import fabric.rw.*
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.filter.Filter
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchQuerySyntax, OpenSearchStore}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort, SortDirection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchKeywordNormalizationSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  import OpenSearchQuerySyntax.*

  case class Doc(name: String, _id: Id[Doc] = Doc.id()) extends Document[Doc]
  object Doc extends DocumentModel[Doc] with JsonConversion[Doc] {
    override implicit val rw: RW[Doc] = RW.gen
    val name = field.index(_.name)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: OpenSearchStore[Doc, Doc.type] = store(Doc)
  }

  "OpenSearch keyword normalization" should {
    "normalize term + prefix queries on .keyword when enabled (trim + lowercase)" in {
      val key = "lightdb.opensearch.Doc.keyword.normalize"
      val prev = Profig(key).opt[String]

      Profig(key).store("true")

      val db = new DB
      val inserted = Doc("  Alice  ", Id("1"))

      val test = for
        _ <- db.init
        _ <- db.docs.transaction { tx =>
          tx.truncate.next(tx.insert(inserted)).next(tx.commit)
        }
        equalsIds <- db.docs.transaction { tx =>
          tx.query
            .filter(_ => Filter.Equals(Doc.name, "alice"))
            .sort(Sort.ByField(Doc.name, SortDirection.Ascending))
            .stream
            .toList
            .map(_.map(_._id.value))
        }
        prefixIds <- db.docs.transaction { tx =>
          tx.query
            .filter(_ => Filter.StartsWith(Doc.name.name, "AL"))
            .stream
            .toList
            .map(_.map(_._id.value))
        }
        _ <- db.dispose
      yield {
        equalsIds shouldBe List("1")
        prefixIds shouldBe List("1")
      }

      test.guarantee {
        Task {
          prev match {
            case Some(v) => Profig(key).store(v)
            case None => Profig(key).remove()
          }
        }
      }
    }
  }
}


