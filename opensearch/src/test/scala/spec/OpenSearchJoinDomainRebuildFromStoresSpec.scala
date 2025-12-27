package spec

import fabric._
import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
import lightdb.id.Id
import lightdb.opensearch.{OpenSearchJoinDomainCoordinator, OpenSearchRebuild, OpenSearchTemplates}
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.store.hashmap.HashMapStore
import lightdb.time.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

@EmbeddedTest
class OpenSearchJoinDomainRebuildFromStoresSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  case class Parent(name: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Parent] = Parent.id()) extends RecordDocument[Parent]
  object Parent extends RecordDocumentModel[Parent] with JsonConversion[Parent] {
    override implicit val rw: RW[Parent] = RW.gen
    val name: I[String] = field.index(_.name)
  }

  case class Child(parentId: Id[Parent],
                   tag: String,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Child] = Child.id()) extends RecordDocument[Child]
  object Child extends RecordDocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    val parentId: I[Id[Parent]] = field.index(_.parentId)
    val tag: I[String] = field.index(_.tag)
  }

  object SourceDB extends LightDB {
    override type SM = HashMapStore.type
    override val storeManager: HashMapStore.type = HashMapStore
    override def directory = None
    override def upgrades = Nil
    lazy val parents = store[Parent, Parent.type](Parent, name = Some("parents_src"))
    lazy val children = store[Child, Child.type](Child, name = Some("children_src"))
  }

  "OpenSearchRebuild (join-domain from stores)" should {
    "rebuild a join-domain index by indexing parent+child stores into the same physical index" in {
      val joinDomain = "join_domain_rebuild"
      val parentStoreName = "Parents"
      val childStoreName = "Children"

      val joinProps = OpenSearchJoinDomainCoordinator.configForStoreNames(
        joinDomain = joinDomain,
        parentStoreName = parentStoreName,
        childStoreName = childStoreName,
        childJoinParentFieldName = "parentId",
        joinFieldName = "__lightdb_join"
      )

      OpenSearchJoinDomainCoordinator.withSysProps(joinProps) {
        val targetDb = new lightdb.LightDB {
          override type SM = lightdb.store.CollectionManager
          override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore
          override def directory = None
          override def upgrades = Nil
          override def name: String = "OpenSearchJoinDomainRebuildFromStoresSpec"
        }

        val cfgParent = OpenSearchConfig.from(targetDb, collectionName = parentStoreName)
        val cfgChild = OpenSearchConfig.from(targetDb, collectionName = childStoreName)
        val client = OpenSearchClient(cfgParent)

        val (readAlias, writeAlias) = OpenSearchJoinDomainCoordinator.joinDomainAliases(
          dbName = targetDb.name,
          joinDomain = joinDomain,
          config = cfgParent
        )

        val indexBody = OpenSearchTemplates.indexBody(
          model = Parent,
          fields = Parent.fields,
          config = cfgParent.copy(joinChildren = List(childStoreName)),
          storeName = parentStoreName,
          maxResultWindow = cfgParent.maxResultWindow
        )

        val test = for {
          _ <- SourceDB.init
          _ <- SourceDB.parents.t.truncate
          _ <- SourceDB.children.t.truncate

          p1 = Parent("p1")
          p2 = Parent("p2")
          _ <- SourceDB.parents.t.insert(List(p1, p2))
          _ <- SourceDB.children.t.insert(List(
            Child(parentId = p1._id, tag = "a"),
            Child(parentId = p1._id, tag = "b"),
            Child(parentId = p2._id, tag = "b")
          ))

          // cleanup any prior physical indices behind the read alias (best effort)
          existing <- client.aliasTargets(readAlias)
          _ <- existing.foldLeft(Task.unit)((acc, idx) => acc.next(client.deleteIndex(idx)))

          newPhysical <- OpenSearchJoinDomainCoordinator.rebuildAndRepointJoinDomainAliasesFromSources(
            client = client,
            dbName = targetDb.name,
            joinDomain = joinDomain,
            config = cfgParent,
            indexBody = indexBody,
            sources = List(
              OpenSearchRebuild.RebuildSource.fromStore(
                storeName = parentStoreName,
                fields = Parent.fields,
                source = SourceDB.parents,
                docConfig = cfgParent
              ),
              OpenSearchRebuild.RebuildSource.fromStore(
                storeName = childStoreName,
                fields = Child.fields,
                source = SourceDB.children,
                docConfig = cfgChild
              )
            ),
            defaultSuffix = "_000001",
            refreshAfter = true
          )

          // Validate that the join field/routing is correct enough for a native has_child query.
          // Match parents that have a child tag == "a" => should return only p1.
          query = obj(
            "size" -> num(10),
            "query" -> obj(
              "has_child" -> obj(
                "type" -> str(childStoreName),
                "query" -> obj("term" -> obj("tag.keyword" -> str("a"))),
                "score_mode" -> str("none")
              )
            )
          )

          hits <- client.search(readAlias, query)
          ids = hits.asObj.value("hits").asObj.value("hits").asArr.value.toList.map { h =>
            h.asObj.value("_source").asObj.value(OpenSearchTemplates.InternalIdField).asString
          }

          _ <- client.deleteIndex(newPhysical)
        } yield {
          ids.toSet shouldBe Set(p1._id.value)
        }

        test
      }
    }
  }
}


