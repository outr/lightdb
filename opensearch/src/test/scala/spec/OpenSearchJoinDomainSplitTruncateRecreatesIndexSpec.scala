package spec

import fabric._
import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion, ParentChildSupport}
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchIndexName, OpenSearchTemplates}
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.store.split.SplitStoreManager
import lightdb.store.hashmap.HashMapStore
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

/**
 * Regression:
 * Join-domain indices are shared by multiple logical collections (parent/child).
 * `truncate` must not wipe other join types in the same physical index.
 */
@EmbeddedTest
class OpenSearchJoinDomainSplitTruncateIsolationSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with OpenSearchTestSupport {

  case class Parent(name: String, _id: Id[Parent] = Parent.id()) extends Document[Parent]
  object Parent extends DocumentModel[Parent] with JsonConversion[Parent] with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen
    override def childStore = DB.children
    override def parentField(childModel: Child.type) = childModel.parentId
    override def joinDomainName(parentStoreName: String): String = "split_join_domain"
    override def joinFieldName: String = "__split_join"
    val name: I[String] = field.index("name", _.name)
  }

  case class Child(parentId: Id[Parent], value: String, _id: Id[Child] = Child.id()) extends Document[Child]
  object Child extends DocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    val parentId: I[Id[Parent]] = field.index("parentId", _.parentId)
    val value: I[String] = field.index("value", _.value)
  }

  object DB extends LightDB {
    override type SM = SplitStoreManager[HashMapStore.type, lightdb.opensearch.OpenSearchStore.type]
    override val storeManager: SM = SplitStoreManager(HashMapStore, lightdb.opensearch.OpenSearchStore)
    override def name: String = "OpenSearchJoinDomainSplitTruncateIsolationSpec"
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val parents = store[Parent, Parent.type](Parent, name = Some("SplitParent"))
    val children = store[Child, Child.type](Child, name = Some("SplitChild"))
  }

  "OpenSearchTransaction.truncate (SplitCollection join-domain)" should {
    "only delete the current store's join type (and preserve other join types)" in {
      val p1 = Parent("p1")
      val p2 = Parent("p2")
      val c1 = Child(parentId = p1._id, value = "c1")
      val c2 = Child(parentId = p2._id, value = "c2")

      val test = for {
        _ <- DB.init
        _ <- DB.parents.transaction(_.truncate)
        _ <- DB.children.transaction(_.truncate)
        _ <- DB.parents.transaction(_.insert(List(p1, p2)))
        _ <- DB.children.transaction(_.insert(List(c1, c2)))

        parentsBefore <- DB.parents.searching.transaction(_.count)
        childrenBefore <- DB.children.searching.transaction(_.count)

        // Truncate parents type only: children should remain.
        _ <- DB.parents.searching.transaction(_.truncate)
        parentsAfterParentTruncate <- DB.parents.searching.transaction(_.count)
        childrenAfterParentTruncate <- DB.children.searching.transaction(_.count)

        // Re-insert parents so we can validate child truncate isolation too.
        _ <- DB.parents.transaction(_.insert(List(p1, p2)))

        // Truncate children type only: parents should remain.
        _ <- DB.children.searching.transaction(_.truncate)
        parentsAfterChildTruncate <- DB.parents.searching.transaction(_.count)
        childrenAfterChildTruncate <- DB.children.searching.transaction(_.count)

        _ <- DB.truncate()
        _ <- DB.dispose
      } yield {
        parentsBefore shouldBe 2
        childrenBefore shouldBe 2

        parentsAfterParentTruncate shouldBe 0
        childrenAfterParentTruncate shouldBe 2

        parentsAfterChildTruncate shouldBe 2
        childrenAfterChildTruncate shouldBe 0
      }

      test
    }
  }
}

