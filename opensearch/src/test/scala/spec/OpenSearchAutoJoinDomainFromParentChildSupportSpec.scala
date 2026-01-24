package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.opensearch.OpenSearchStore
import lightdb.opensearch.client.OpenSearchConfig
import lightdb.store.Collection
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchAutoJoinDomainFromParentChildSupportSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with OpenSearchTestSupport {

  object DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager: lightdb.store.CollectionManager = OpenSearchStore
    override def name: String = "OpenSearchAutoJoinDomainFromParentChildSupportSpec"
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    // Parent is defined first, but child is still created before init begins (store vals are evaluated during DB construction).
    val parents: Collection[Parent, Parent.type] = store(Parent, name = Some("AutoParent"))
    val children: Collection[Child, Child.type] = store(Child, name = Some("AutoChild"))
  }

  case class Parent(name: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Parent] = Parent.id()) extends RecordDocument[Parent]

  object Parent
      extends RecordDocumentModel[Parent]
      with JsonConversion[Parent]
      with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen

    override def childStore: Collection[Child, Child.type] = DB.children
    override def parentField(childModel: Child.type): Field[Child, Id[Parent]] = childModel.parentId

    // Prove this is controllable without Profig keys.
    override def joinDomainName(parentStoreName: String): String = "auto_join_domain"
    override def joinFieldName: String = "__auto_join"

    val name: F[String] = field("name", _.name)
  }

  case class Child(parentId: Id[Parent],
                   state: Option[String] = None,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Child] = Child.id()) extends RecordDocument[Child]

  object Child extends RecordDocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    val parentId: F[Id[Parent]] = field("parentId", _.parentId)
    val state: F[Option[String]] = field("state", _.state)
  }

  "OpenSearchStore auto join-domain config" should {
    "auto-register join-domain config from ParentChildSupport without any Profig join keys" in {
      val test = for
        _ <- DB.init
        // Trigger config evaluation via store init, then validate it via OpenSearchConfig.from.
        parentCfg = OpenSearchConfig.from(DB, "AutoParent")
        childCfg = OpenSearchConfig.from(DB, "AutoChild")
        _ <- DB.dispose
      yield {
        parentCfg.joinDomain shouldBe Some("auto_join_domain")
        parentCfg.joinRole shouldBe Some("parent")
        parentCfg.joinChildren shouldBe List("AutoChild")
        parentCfg.joinFieldName shouldBe "__auto_join"

        childCfg.joinDomain shouldBe Some("auto_join_domain")
        childCfg.joinRole shouldBe Some("child")
        childCfg.joinParentField shouldBe Some("parentId")
        childCfg.joinFieldName shouldBe "__auto_join"
      }

      test
    }
  }
}


