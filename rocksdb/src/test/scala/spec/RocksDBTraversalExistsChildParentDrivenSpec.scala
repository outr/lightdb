package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.LightDB
import lightdb.traversal.store.TraversalManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.AsyncTaskSpec

import java.nio.file.Path

@EmbeddedTest
class RocksDBTraversalExistsChildParentDrivenSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    // Enable traversal-native ExistsChild so Query.prepared won't resolve it.
    System.setProperty("lightdb.traversal.existsChild.native", "true")
    // Enable parent-driven page-only probing.
    System.setProperty("lightdb.traversal.existsChild.parentDriven", "true")
    System.setProperty("lightdb.traversal.existsChild.parentDriven.maxParents", "4")
    // If planner path accidentally gets used, it will blow up for this query (too many parents).
    System.setProperty("lightdb.existsChild.maxParentIds", "1")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally {
      System.clearProperty("lightdb.traversal.existsChild.native")
      System.clearProperty("lightdb.traversal.existsChild.parentDriven")
      System.clearProperty("lightdb.traversal.existsChild.parentDriven.maxParents")
      System.clearProperty("lightdb.existsChild.maxParentIds")
    }
  }

  private lazy val specName: String = getClass.getSimpleName
  override def traversalStoreManager: TraversalManager = super.traversalStoreManager

  object DB extends LightDB {
    override type SM = TraversalManager
    override val storeManager: TraversalManager = traversalStoreManager

    override def name: String = specName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val parents: S[Parent, Parent.type] = store(Parent)
    val children: S[Child, Child.type] = store(Child)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Parent(name: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Parent] = Parent.id()) extends RecordDocument[Parent]

  object Parent extends RecordDocumentModel[Parent]
      with JsonConversion[Parent]
      with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen

    override def childStore: Collection[Child, Child.type] = DB.children
    override def parentField(childModel: Child.type): Field[Child, Id[Parent]] = childModel.parentId

    val name: I[String] = field.index(_.name)
  }

  case class Child(parentId: Id[Parent],
                   state: Option[String] = None,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Child] = Child.id()) extends RecordDocument[Child]

  object Child extends RecordDocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen
    val parentId: I[Id[Parent]] = field.index(_.parentId)
    val state: I[Option[String]] = field.index(_.state)
  }

  specName should {
    "use parent-driven probing for page-only ExistsChild when parent side is selectively seeded" in {
      val alpha = Parent("Alpha", _id = Id("alpha"))
      val bravo = Parent("Bravo", _id = Id("bravo"))
      val kids = List(
        Child(alpha._id, state = Some("WY")),
        Child(bravo._id, state = Some("UT"))
      )

      for
        _ <- DB.init
        _ <- DB.truncate()
        _ <- DB.parents.transaction(_.insert(List(alpha, bravo)))
        _ <- DB.children.transaction(_.insert(kids))
        ids <- DB.parents.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(1)
            .filter(_.name === "Alpha") // small parent seed
            .filter(_.childFilter(_.state === Some("WY")))
            .id
            .toList
        }
      yield {
        ids shouldBe List(alpha._id)
      }
    }
  }
}


