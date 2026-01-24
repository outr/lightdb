package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.store.StoreMode
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort, SortDirection}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchIdFieldSortSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  // Use unique store/model names to avoid picking up join-domain Profig config from other suites (e.g. a store named "Child").
  case class IdFieldSortParent(_id: Id[IdFieldSortParent] = IdFieldSortParent.id()) extends Document[IdFieldSortParent]
  object IdFieldSortParent extends DocumentModel[IdFieldSortParent] with JsonConversion[IdFieldSortParent] {
    override implicit val rw: RW[IdFieldSortParent] = RW.gen
  }

  case class IdFieldSortChild(parentId: Id[IdFieldSortParent], _id: Id[IdFieldSortChild] = IdFieldSortChild.id()) extends Document[IdFieldSortChild]
  object IdFieldSortChild extends DocumentModel[IdFieldSortChild] with JsonConversion[IdFieldSortChild] {
    override implicit val rw: RW[IdFieldSortChild] = RW.gen
    val parentId: I[Id[IdFieldSortParent]] = field.index("parentId", _.parentId)
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val children: OpenSearchStore[IdFieldSortChild, IdFieldSortChild.type] = store(IdFieldSortChild)
  }

  "OpenSearch sort-by-Id-field" should {
    "support Sort.ByField(Model.someId) without requiring a `.keyword` subfield mapping" in {
      val db = new DB
      val c1 = IdFieldSortChild(parentId = Id[IdFieldSortParent]("p2"), _id = Id[IdFieldSortChild]("c1"))
      val c2 = IdFieldSortChild(parentId = Id[IdFieldSortParent]("p1"), _id = Id[IdFieldSortChild]("c2"))

      val test = for
        _ <- db.init
        _ <- db.children.transaction { tx =>
          tx.truncate.next(tx.insert(List(c1, c2))).next(tx.commit)
        }
        ids <- db.children.transaction { tx =>
          tx.query
            .sort(Sort.ByField(IdFieldSortChild.parentId, SortDirection.Ascending))
            .docs
            .stream
            .map(_._id.value)
            .toList
        }
        _ <- db.dispose
      yield {
        ids shouldBe List("c2", "c1")
      }

      test
    }
  }
}

