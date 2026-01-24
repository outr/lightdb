package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion, ParentChildSupport}
import lightdb.filter.Filter
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchQuerySyntax, OpenSearchStore}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchJoinChildrenDefaultSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  import OpenSearchQuerySyntax.*

  case class Parent(name: String, _id: Id[Parent] = Parent.id()) extends Document[Parent]
  case class Child(parentId: Id[Parent], value: String, _id: Id[Child] = Child.id()) extends Document[Child]

  object Child extends DocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    val parentId: I[Id[Parent]] = field.index(_.parentId)
    val value: I[String] = field.index(_.value)
  }

  object Parent extends DocumentModel[Parent] with JsonConversion[Parent] with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen
    val name: I[String] = field.index(_.name)

    // wired below via lazy db
    override def childStore: lightdb.store.Collection[Child, Child.type] = db.children
    override def parentField(childModel: Child.type) = childModel.parentId
  }

  class DB extends LightDB {
    override type SM = OpenSearchStore.type
    override val storeManager: OpenSearchStore.type = OpenSearchStore
    override def name: String = "OpenSearchJoinChildrenDefaultSpec"
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val parents: OpenSearchStore[Parent, Parent.type] = store(Parent)
    val children: OpenSearchStore[Child, Child.type] = store(Child)
  }

  private lazy val db = new DB

  "OpenSearch join mapping" should {
    "default joinChildren from ParentChildSupport when joinRole=parent and joinChildren is not configured" in {
      val joinDomainKey = "lightdb.opensearch.Parent.joinDomain"
      val joinDomainChildKey = "lightdb.opensearch.Child.joinDomain"
      val joinRoleParentKey = "lightdb.opensearch.Parent.joinRole"
      val joinRoleChildKey = "lightdb.opensearch.Child.joinRole"
      val joinParentFieldKey = "lightdb.opensearch.Child.joinParentField"
      val joinChildrenKey = "lightdb.opensearch.Parent.joinChildren"

      val prev = Map(
        joinDomainKey -> Profig(joinDomainKey).opt[String],
        joinDomainChildKey -> Profig(joinDomainChildKey).opt[String],
        joinRoleParentKey -> Profig(joinRoleParentKey).opt[String],
        joinRoleChildKey -> Profig(joinRoleChildKey).opt[String],
        joinParentFieldKey -> Profig(joinParentFieldKey).opt[String],
        joinChildrenKey -> Profig(joinChildrenKey).opt[String]
      )

      Profig(joinDomainKey).store("join-default")
      Profig(joinDomainChildKey).store("join-default")
      Profig(joinRoleParentKey).store("parent")
      Profig(joinRoleChildKey).store("child")
      Profig(joinParentFieldKey).store("parentId")
      Profig(joinChildrenKey).remove() // critical: no explicit joinChildren

      val p = Parent("p1", Id("p1"))
      val c = Child(parentId = p._id, value = "c1", _id = Id("c1"))

      val test = for
        _ <- db.init
        _ <- db.parents.transaction { tx =>
          tx.truncate.next(tx.insert(p)).next(tx.commit)
        }
        _ <- db.children.transaction { tx =>
          // In a join-domain, truncating the child store would wipe the shared index (including parents),
          // so we only truncate via the join-parent store above.
          tx.insert(c).next(tx.commit)
        }
        matched <- db.parents.transaction { tx =>
          tx.query
            .filter(_.childFilter(_.value === "c1"))
            .stream
            .toList
        }
        _ <- db.dispose
      yield {
        matched.map(_._id.value) shouldBe List("p1")
      }

      test.guarantee {
        Task {
          prev.foreach {
            case (k, Some(v)) => Profig(k).store(v)
            case (k, None) => Profig(k).remove()
          }
        }
      }
    }
  }
}


