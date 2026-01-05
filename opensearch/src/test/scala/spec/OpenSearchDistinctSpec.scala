package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort, SortDirection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchDistinctSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class DistinctDoc(group: String, weight: Int, _id: Id[DistinctDoc] = DistinctDoc.id()) extends Document[DistinctDoc]

  object DistinctDoc extends DocumentModel[DistinctDoc] with JsonConversion[DistinctDoc] {
    override implicit val rw: RW[DistinctDoc] = RW.gen
    val group: I[String] = field.index(_.group)
    val weight: I[Int] = field.index(_.weight)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: OpenSearchStore[DistinctDoc, DistinctDoc.type] = store(DistinctDoc)
  }

  "OpenSearch distinct" should {
    "return distinct values for a field and support count" in {
      val db = new DB
      val records = List(
        DistinctDoc("a", 1, Id("1")),
        DistinctDoc("a", 2, Id("2")),
        DistinctDoc("b", 3, Id("3")),
        DistinctDoc("b", 4, Id("4")),
        DistinctDoc("c", 5, Id("5"))
      )

      val test = for {
        _ <- db.init
        _ <- db.docs.transaction { tx =>
          tx.truncate.next(tx.insert(records)).next(tx.commit)
        }
        // Unfiltered distinct groups
        groups <- db.docs.transaction { tx =>
          tx.query
            .sort(Sort.ByField(DistinctDoc.group, SortDirection.Ascending))
            .distinct(_.group, pageSize = 2)
            .toList
        }
        // Count distinct groups
        groupCount <- db.docs.transaction { tx =>
          tx.query.distinct(_.group, pageSize = 2).count
        }
        // Filtered distinct groups (weight >= 4 -> b,c)
        filtered <- db.docs.transaction { tx =>
          tx.query
            .filter(_.weight >= 4)
            .distinct(_.group, pageSize = 2)
            .toList
        }
        _ <- db.dispose
      } yield {
        groups.toSet should be(Set("a", "b", "c"))
        groupCount should be(3)
        filtered.toSet should be(Set("b", "c"))
      }

      test
    }
  }
}

