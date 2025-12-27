package spec

import fabric._
import fabric.rw._
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.opensearch.OpenSearchRebuild
import lightdb.opensearch.client.{OpenSearchClient, OpenSearchConfig}
import lightdb.store.Collection
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class OpenSearchJoinDomainPartialRebuildSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  Profig("lightdb.opensearch.Parent.joinDomain").store("join_partial_rebuild")
  Profig("lightdb.opensearch.Parent.joinRole").store("parent")
  Profig("lightdb.opensearch.Parent.joinChildren").store("Child")

  Profig("lightdb.opensearch.Child.joinDomain").store("join_partial_rebuild")
  Profig("lightdb.opensearch.Child.joinRole").store("child")
  Profig("lightdb.opensearch.Child.joinParentField").store("parentId")

  private val alpha = Parent(name = "Alpha")
  private val bravo = Parent(name = "Bravo")

  private val children = List(
    Child(parentId = alpha._id, state = Some("WY")),
    Child(parentId = alpha._id, range = Some("68W")),
    Child(parentId = bravo._id, state = Some("WY"), range = Some("69W"))
  )

  "OpenSearch join-domain partial rebuild" should {
    "delete by parent id (parent + children) and reindex replacement docs" in {
      val cfg = OpenSearchConfig.from(DB, "Parent")
      val client = OpenSearchClient(cfg)
      val joinIndex = lightdb.opensearch.OpenSearchIndexName.default(DB.name, "join_partial_rebuild", cfg)
      val joinFieldName = cfg.joinFieldName

      def parentSource(id: String, name: String): Json = obj(
        lightdb.opensearch.OpenSearchTemplates.InternalIdField -> str(id),
        "name" -> str(name),
        joinFieldName -> obj("name" -> str("Parent"))
      )

      def childSource(id: String, parentId: String, state: Option[String], range: Option[String]): Json = {
        val base = List(
          lightdb.opensearch.OpenSearchTemplates.InternalIdField -> str(id),
          "parentId" -> str(parentId),
          joinFieldName -> obj("name" -> str("Child"), "parent" -> str(parentId))
        )
        val extra = List(
          state.map(v => "state" -> str(v)),
          range.map(v => "range" -> str(v))
        ).flatten
        obj((base ++ extra): _*)
      }

      val test = for {
        _ <- DB.init
        _ <- DB.parents.transaction(_.truncate)
        _ <- DB.children.transaction(_.truncate)
        _ <- DB.parents.transaction(_.insert(List(alpha, bravo)))
        _ <- DB.children.transaction(_.insert(children))

        before <- DB.parents.transaction(_.query.filter(_.name === "Alpha").id.toList)

        // Replace Alpha with Alpha2 and change its children.
        // This uses low-level OpenSearchRebuild to simulate "system-of-record streaming JSON".
        replacementDocs = rapid.Stream.emits(List(
          OpenSearchRebuild.RebuildDoc(
            id = alpha._id.value,
            source = parentSource(alpha._id.value, "Alpha2"),
            routing = Some(alpha._id.value)
          ),
          OpenSearchRebuild.RebuildDoc(
            id = "c_alpha_1",
            source = childSource("c_alpha_1", alpha._id.value, state = Some("UT"), range = None),
            routing = Some(alpha._id.value)
          ),
          OpenSearchRebuild.RebuildDoc(
            id = "c_alpha_2",
            source = childSource("c_alpha_2", alpha._id.value, state = None, range = Some("70W")),
            routing = Some(alpha._id.value)
          )
        ))
        deleted <- OpenSearchRebuild.rebuildJoinDomainByParentIds(
          client = client,
          index = joinIndex,
          joinFieldName = joinFieldName,
          parentStoreName = "Parent",
          childParentFields = Map("Child" -> "parentId"),
          parentIds = List(alpha._id.value),
          docs = replacementDocs,
          config = cfg,
          refreshAfter = true
        )

        afterAlpha <- DB.parents.transaction(_.query.filter(_.name === "Alpha2").id.toList)
        afterBravo <- DB.parents.transaction(_.query.filter(_.name === "Bravo").id.toList)
        // verify join still works: Alpha2 now has UT state on a child
        alpha2ByChild <- DB.parents.transaction(_.query.filter(_.childFilter(_.state === Some("UT"))).id.toList)

        _ <- DB.truncate()
        _ <- DB.dispose
      } yield {
        before.toSet shouldBe Set(alpha._id)
        deleted should be >= 1
        afterAlpha.toSet shouldBe Set(alpha._id)
        afterBravo.toSet shouldBe Set(bravo._id)
        alpha2ByChild.toSet shouldBe Set(alpha._id)
      }

      test
    }
  }

  object DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore

    override def name: String = "OpenSearchJoinDomainPartialRebuildSpec"
    override lazy val directory: Option[Path] = Some(Path.of("db/OpenSearchJoinDomainPartialRebuildSpec"))

    val parents: Collection[Parent, Parent.type] = store(Parent)
    val children: Collection[Child, Child.type] = store(Child)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Parent(name: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Parent] = Parent.id()) extends RecordDocument[Parent]
  object Parent extends RecordDocumentModel[Parent] with JsonConversion[Parent] with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen

    override def childStore: Collection[Child, Child.type] = DB.children

    override def parentField(childModel: Child.type): Field[Child, Id[Parent]] = childModel.parentId

    val name: I[String] = field.index(_.name)
  }

  case class Child(parentId: Id[Parent],
                   state: Option[String] = None,
                   range: Option[String] = None,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Child] = Child.id()) extends RecordDocument[Child]
  object Child extends RecordDocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen

    val parentId: I[Id[Parent]] = field.index(_.parentId)
    val state: I[Option[String]] = field.index(_.state)
    val range: I[Option[String]] = field.index(_.range)
  }
}


