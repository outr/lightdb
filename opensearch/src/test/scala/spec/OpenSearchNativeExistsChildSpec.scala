package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.store.Collection
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class OpenSearchNativeExistsChildSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  // Configure a join-domain shared index:
  // - Parent store is the join parent type and declares allowed children
  // - Child store is the join child type and declares which field contains parent id for routing/join
  Profig("lightdb.opensearch.Parent.joinDomain").store("native_exists_child")
  Profig("lightdb.opensearch.Parent.joinRole").store("parent")
  Profig("lightdb.opensearch.Parent.joinChildren").store("Child")

  Profig("lightdb.opensearch.Child.joinDomain").store("native_exists_child")
  Profig("lightdb.opensearch.Child.joinRole").store("child")
  Profig("lightdb.opensearch.Child.joinParentField").store("parentId")

  private val alpha = Parent(name = "Alpha")
  private val bravo = Parent(name = "Bravo")

  private val children = List(
    Child(parentId = alpha._id, state = Some("WY")),
    Child(parentId = alpha._id, range = Some("68W")),
    Child(parentId = bravo._id, state = Some("WY"), range = Some("69W"))
  )

  "OpenSearch native ExistsChild" should {
    "match parents using has_child (no planner resolution)" in {
      val test = for
        _ <- DB.init
        _ <- DB.parents.transaction(_.truncate)
        _ <- DB.children.transaction(_.truncate)
        _ <- DB.parents.transaction(_.insert(List(alpha, bravo)))
        _ <- DB.children.transaction(_.insert(children))
        ids <- DB.parents.transaction { tx =>
          tx.query
            .filter(_.childFilter(_.state === Some("WY")))
            .id
            .toList
        }
        _ <- DB.truncate()
        _ <- DB.dispose
      yield {
        ids.toSet should be(Set(alpha._id, bravo._id))
      }
      test
    }

    "apply minDocScore predictably for filter-only has_child queries" in {
      val test = for
        _ <- DB.init
        _ <- DB.parents.transaction(_.truncate)
        _ <- DB.children.transaction(_.truncate)
        _ <- DB.parents.transaction(_.insert(List(alpha, bravo)))
        _ <- DB.children.transaction(_.insert(children))
        ok <- DB.parents.transaction { tx =>
          tx.query
            .filter(_.childFilter(_.state === Some("WY")))
            .minDocScore(0.5)
            .id
            .toList
        }
        none <- DB.parents.transaction { tx =>
          tx.query
            .filter(_.childFilter(_.state === Some("WY")))
            .minDocScore(1.1)
            .id
            .toList
        }
        _ <- DB.truncate()
        _ <- DB.dispose
      yield {
        ok.toSet should be(Set(alpha._id, bravo._id))
        none shouldBe Nil
      }
      test
    }
  }

  object DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore

    override def name: String = "OpenSearchNativeExistsChildSpec"
    override lazy val directory: Option[Path] = Some(Path.of("db/OpenSearchNativeExistsChildSpec"))

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



