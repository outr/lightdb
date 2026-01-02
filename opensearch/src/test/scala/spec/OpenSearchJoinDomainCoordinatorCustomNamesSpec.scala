package spec

import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion, ParentChildSupport}
import lightdb.filter.Filter
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchJoinDomainCoordinator, OpenSearchQuerySyntax, OpenSearchStore}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchJoinDomainCoordinatorCustomNamesSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  import OpenSearchQuerySyntax._

  case class Parent(name: String, _id: Id[Parent] = Parent.id()) extends Document[Parent]
  case class Child(parentId: Id[Parent], value: String, _id: Id[Child] = Child.id()) extends Document[Child]

  object Child extends DocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    val parentId = field.index(_.parentId)
    val value = field.index(_.value)
  }

  object Parent extends DocumentModel[Parent] with JsonConversion[Parent] with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen
    val name = field.index(_.name)
    override def childStore = db.children
    override def parentField(childModel: Child.type) = childModel.parentId
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil
    override def name: String = "OpenSearchJoinDomainCoordinatorCustomNamesSpec"

    // custom store names (not equal to model names)
    val parents: OpenSearchStore[Parent, Parent.type] = store(Parent, name = Some("ParentsCustom"))
    val children: OpenSearchStore[Child, Child.type] = store(Child, name = Some("ChildrenCustom"))
  }

  private lazy val db = new DB

  "OpenSearchJoinDomainCoordinator" should {
    "configure join-domain using store names only (supports custom store names)" in {
      val kLogRequests = "lightdb.opensearch.logRequests"
      val kTimeout = "lightdb.opensearch.requestTimeoutMillis"
      val prevLogRequests = Profig(kLogRequests).opt[Boolean]
      val prevTimeout = Profig(kTimeout).opt[Long]

      // This spec does a bit more OpenSearch setup work (custom names + join-domain). Increase timeouts and enable
      // request logging for easier debugging when running under CI / slow docker.
      Profig(kLogRequests).store(true)
      Profig(kTimeout).store(30000L)

      val joinDomain = "coord-custom"
      val parentStoreName = "ParentsCustom"
      val childStoreName = "ChildrenCustom"

      val cfg = OpenSearchJoinDomainCoordinator.configForStoreNames(
        joinDomain = joinDomain,
        parentStoreName = parentStoreName,
        childStoreName = childStoreName,
        childJoinParentFieldName = "parentId"
      )
      OpenSearchJoinDomainCoordinator.withSysProps(cfg) {
        val p = Parent("p1", Id("p1"))
        val c = Child(parentId = p._id, value = "c1", _id = Id("c1"))

        val test = for {
          _ <- db.init
          _ <- db.parents.transaction { tx =>
            tx.truncate.next(tx.insert(p)).next(tx.commit)
          }
          _ <- db.children.transaction { tx =>
            // Truncate now drops/recreates the entire join-domain index; don't truncate from the child store here.
            tx.insert(c).next(tx.commit)
          }
          matched <- db.parents.transaction { tx =>
            tx.query
              .filter(_.childFilter(_.value === "c1"))
              .stream
              .toList
          }
          _ <- db.dispose
        } yield {
          matched.map(_._id.value) shouldBe List("p1")
        }

        test.guarantee(Task {
          prevLogRequests match {
            case Some(v) => Profig(kLogRequests).store(v)
            case None => Profig(kLogRequests).remove()
          }
          prevTimeout match {
            case Some(v) => Profig(kTimeout).store(v)
            case None => Profig(kTimeout).remove()
          }
        })
      }
    }
  }
}


