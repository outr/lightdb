package spec

import fabric.rw._
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.id.Id
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
class OpenSearchJoinDomainInferenceSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers with OpenSearchTestSupport {
  // Only configure the join-domain on the parent store and provide the child-parent field mapping there.
  // Child store should infer:
  // - joinDomain
  // - joinRole=child
  // - joinParentField
  Profig("lightdb.opensearch.ParentInfer.joinDomain").store("join_infer_domain")
  Profig("lightdb.opensearch.ParentInfer.joinChildren").store("ChildInfer")
  Profig("lightdb.opensearch.ParentInfer.joinChildParentFields").store("ChildInfer:parentId")
  // Ensure we are not explicitly configuring the child keys (the point of this spec).
  Profig("lightdb.opensearch.ChildInfer.joinDomain").remove()
  Profig("lightdb.opensearch.ChildInfer.joinRole").remove()
  Profig("lightdb.opensearch.ChildInfer.joinParentField").remove()

  private val alpha = ParentInfer(name = "Alpha")
  private val bravo = ParentInfer(name = "Bravo")

  private val children = List(
    ChildInfer(parentId = alpha._id, state = Some("WY")),
    ChildInfer(parentId = alpha._id, range = Some("68W")),
    ChildInfer(parentId = bravo._id, state = Some("WY"), range = Some("69W"))
  )

  "OpenSearch join-domain config inference" should {
    "infer child join config from parent joinChildParentFields and execute native has_child" in {
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
  }

  object DB extends LightDB {
    override type SM = lightdb.store.CollectionManager
    override val storeManager: lightdb.store.CollectionManager = lightdb.opensearch.OpenSearchStore

    override def name: String = "OpenSearchJoinDomainInferenceSpec"
    override lazy val directory: Option[Path] = Some(Path.of("db/OpenSearchJoinDomainInferenceSpec"))

    val parents: Collection[ParentInfer, ParentInfer.type] = store(ParentInfer)
    val children: Collection[ChildInfer, ChildInfer.type] = store(ChildInfer)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class ParentInfer(name: String,
                         created: Timestamp = Timestamp(),
                         modified: Timestamp = Timestamp(),
                         _id: Id[ParentInfer] = ParentInfer.id()) extends RecordDocument[ParentInfer]
  object ParentInfer extends RecordDocumentModel[ParentInfer] with JsonConversion[ParentInfer] with ParentChildSupport[ParentInfer, ChildInfer, ChildInfer.type] {
    override implicit val rw: RW[ParentInfer] = RW.gen

    override def childStore: Collection[ChildInfer, ChildInfer.type] = DB.children

    override def parentField(childModel: ChildInfer.type): Field[ChildInfer, Id[ParentInfer]] = childModel.parentId

    val name: I[String] = field.index(_.name)
  }

  case class ChildInfer(parentId: Id[ParentInfer],
                        state: Option[String] = None,
                        range: Option[String] = None,
                        created: Timestamp = Timestamp(),
                        modified: Timestamp = Timestamp(),
                        _id: Id[ChildInfer] = ChildInfer.id()) extends RecordDocument[ChildInfer]
  object ChildInfer extends RecordDocumentModel[ChildInfer] with JsonConversion[ChildInfer] {
    override implicit val rw: RW[ChildInfer] = RW.gen

    val parentId: I[Id[ParentInfer]] = field.index(_.parentId)
    val state: I[Option[String]] = field.index(_.state)
    val range: I[Option[String]] = field.index(_.range)
  }
}


