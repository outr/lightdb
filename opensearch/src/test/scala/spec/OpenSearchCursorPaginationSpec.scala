package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchQuerySyntax, OpenSearchStore}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort, SortDirection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchCursorPaginationSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  import OpenSearchQuerySyntax._

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
    "page through results via search_after with stable ordering" in {
      val db = new DB
      val records = List(
        CursorDoc("a", 1, Id("a")),
        CursorDoc("b", 1, Id("b")),
        CursorDoc("c", 2, Id("c")),
        CursorDoc("d", 2, Id("d")),
        CursorDoc("e", 2, Id("e")),
        CursorDoc("f", 3, Id("f")),
        CursorDoc("g", 3, Id("g"))
      )

      def loop(q: lightdb.Query[CursorDoc, CursorDoc.type, CursorDoc], cursor: Option[String], acc: List[String]): Task[List[String]] =
        q.cursorPage(cursorToken = cursor, pageSize = 3).flatMap { page =>
          page.results.list.flatMap { docs =>
            val names = docs.map(_.name)
            page.nextCursorToken match {
              case Some(next) => loop(q, Some(next), acc ::: names)
              case None => Task.pure(acc ::: names)
            }
          }
        }

      val expected = records.sortBy(r => (r.weight, r._id.value)).map(_.name)

      val test = for {
        _ <- db.init
        _ <- db.docs.transaction { tx =>
          tx.truncate.next(tx.insert(records)).next(tx.commit)
        }
        names <- db.docs.transaction { tx =>
          val q = tx.query
            .sort(Sort.ByField(CursorDoc.weight, SortDirection.Ascending))
            .sort(Sort.IndexOrder)
          loop(q, None, Nil)
        }
        _ <- db.dispose
      } yield {
        names should be(expected)
      }

      test
    }
  }
}



