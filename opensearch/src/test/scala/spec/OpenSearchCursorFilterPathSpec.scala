package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchQuerySyntax, OpenSearchStore}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort, SortDirection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchCursorFilterPathSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  import OpenSearchQuerySyntax._

  // Intentionally omit `hits.hits.sort` so cursor paging must force-include it.
  // IMPORTANT: use store-specific key so we don't affect the rest of the OpenSearch test suite (facets, grouping, etc.).
  Profig("lightdb.opensearch.CursorDoc.search.filterPath").store("hits.hits._id,hits.hits._source,hits.hits._score")

  case class CursorDoc(name: String, weight: Int, _id: Id[CursorDoc] = CursorDoc.id()) extends Document[CursorDoc]
  object CursorDoc extends DocumentModel[CursorDoc] with JsonConversion[CursorDoc] {
    override implicit val rw: RW[CursorDoc] = RW.gen
    val name: I[String] = field.index(_.name)
    val weight: I[Int] = field.index(_.weight)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val docs: OpenSearchStore[CursorDoc, CursorDoc.type] = store(CursorDoc)
  }

  "OpenSearch cursor pagination" should {
    "work even when filter_path is configured without hits.hits.sort" in {
      val db = new DB

      val records = List(
        CursorDoc("a", 1, Id("a")),
        CursorDoc("b", 1, Id("b")),
        CursorDoc("c", 2, Id("c")),
        CursorDoc("d", 2, Id("d")),
        CursorDoc("e", 2, Id("e"))
      )

      val expected = records.sortBy(r => (r.weight, r._id.value)).map(_.name)

      val test = for
        _ <- db.init
        _ <- db.docs.transaction { tx =>
          tx.truncate.next(tx.insert(records)).next(tx.commit)
        }
        results <- db.docs.transaction { tx =>
          val q = tx.query
            .sort(Sort.ByField(CursorDoc.weight, SortDirection.Ascending))
            .sort(Sort.IndexOrder)

          for
            page1 <- q.cursorPage(cursorToken = None, pageSize = 2)
            p1 <- page1.results.list
            next <- Task.pure(page1.nextCursorToken)
            page2 <- q.cursorPage(cursorToken = next, pageSize = 2)
            p2 <- page2.results.list
            page3 <- q.cursorPage(cursorToken = page2.nextCursorToken, pageSize = 2)
            p3 <- page3.results.list
          yield (p1.map(_.name) ::: p2.map(_.name) ::: p3.map(_.name), page1.nextCursorToken)
        }
        _ <- db.dispose
      yield {
        val (names, firstCursor) = results
        firstCursor should not be empty
        names.distinct should be(names)
        names should be(expected)
      }

      test
    }
  }
}


