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
 * In SplitCollection DBs, the OpenSearchStore instances (searching) are nested under SplitCollection and may not be
 * present in LightDB.stores. `OpenSearchTransaction.truncate` must still recreate join-domain indices using the
 * JOIN-PARENT store mapping (so the join field is `type: join`, and facet `__facet` fields are keyword).
 */
@EmbeddedTest
class OpenSearchJoinDomainSplitTruncateRecreatesIndexSpec
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
    override def name: String = "OpenSearchJoinDomainSplitTruncateRecreatesIndexSpec"
    override def directory = None
    override def upgrades: List[DatabaseUpgrade] = Nil

    val parents = store[Parent, Parent.type](Parent, name = Some("SplitParent"))
    val children = store[Child, Child.type](Child, name = Some("SplitChild"))
  }

  "OpenSearchTransaction.truncate (SplitCollection join-domain)" should {
    "delete and recreate the shared index using the join-parent mapping (type: join)" in {
      // Force join-domain registry/config to initialize by touching the searching store.
      val indexName = DB.children.searching.readIndexName
      val config = OpenSearchConfig.from(DB, collectionName = DB.children.searching.name)
      val client = OpenSearchClient(config)
      val joinFieldName = "__split_join"

      // Create a legacy/bad index: dynamic mapping, then index a child doc which maps join field as an object.
      val badDoc = obj(
        OpenSearchTemplates.InternalIdField -> str("c1"),
        "parentId" -> str("p1"),
        "value" -> str("v1"),
        joinFieldName -> obj("name" -> str("SplitChild"), "parent" -> str("p1"))
      )

      val test = for {
        _ <- client.deleteIndex(indexName)
        _ <- client.createIndex(indexName, obj("mappings" -> obj("dynamic" -> bool(true))))
        _ <- client.indexDoc(indexName, "c1", badDoc, refresh = Some("true"))

        _ <- DB.init
        // Trigger truncate via the searching store transaction (what SplitCollection.reIndex does).
        _ <- DB.children.searching.transaction(_.truncate)

        // If truncate recreated the index using the join-parent template, OpenSearch will accept parent-style join values (string)
        // and will reject them if the join field is still mapped as an object.
        _ <- client.indexDoc(
          index = indexName,
          id = "p1",
          source = obj(
            OpenSearchTemplates.InternalIdField -> str("p1"),
            "name" -> str("parent-name"),
            joinFieldName -> str("SplitParent")
          ),
          refresh = Some("true")
        )
        _ <- DB.dispose
        _ <- client.deleteIndex(indexName)
      } yield {
        succeed
      }

      test
    }
  }
}

