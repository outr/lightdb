package spec

import fabric.rw._
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
class RocksDBTraversalExistsChildNativeFullSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    // Force traversal-native ExistsChild handling + enable the "nativeFull" path.
    System.setProperty("lightdb.traversal.existsChild.native", "true")
    System.setProperty("lightdb.traversal.existsChild.nativeFull", "true")
    System.setProperty("lightdb.traversal.existsChild.nativeFull.maxParentIds", "1000")

    // Make the planner fallback path fail if it gets used (ExistsChild.resolve reads sys.props).
    System.setProperty("lightdb.existsChild.maxParentIds", "1")

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally {
      System.clearProperty("lightdb.traversal.existsChild.native")
      System.clearProperty("lightdb.traversal.existsChild.nativeFull")
      System.clearProperty("lightdb.traversal.existsChild.nativeFull.maxParentIds")
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
    "resolve ExistsChild natively even when the planner maxParentIds would fail (non page-only, countTotal=true)" in {
      val alpha = Parent("Alpha", _id = Id("alpha"))
      val bravo = Parent("Bravo", _id = Id("bravo"))
      val charlie = Parent("Charlie", _id = Id("charlie"))

      val kids = List(
        Child(parentId = alpha._id, state = Some("WY")),
        Child(parentId = bravo._id, state = Some("WY")),
        Child(parentId = charlie._id, state = Some("UT"))
      )

      for {
        _ <- DB.init
        _ <- DB.truncate()
        _ <- DB.parents.transaction(_.insert(List(alpha, bravo, charlie)))
        _ <- DB.children.transaction(_.insert(kids))
        results <- DB.parents.transaction { tx =>
          tx.query
            .filter(_.childFilter(_.state === Some("WY")))
            .countTotal(true)
            .id
            .search
        }
        ids <- results.list
      } yield {
        results.total shouldBe Some(2)
        ids.toSet shouldBe Set(alpha._id, bravo._id)
      }
    }
  }
}


