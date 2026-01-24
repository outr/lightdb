package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.filter.FilterExtras
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import lightdb.traversal.store.TraversalManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import profig.Profig
import rapid.AsyncTaskSpec

import java.nio.file.Path

@EmbeddedTest
class RocksDBTraversalExistsChildEarlyTerminateSpec
    extends AsyncWordSpec
    with AsyncTaskSpec
    with Matchers
    with TraversalRocksDBWrappedManager
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    System.setProperty("lightdb.traversal.existsChild.native", "true")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally System.clearProperty("lightdb.traversal.existsChild.native")
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

  specName should {
    "early-terminate ExistsChild (single) without paging countTotal" in {
      val alpha = Parent("Alpha", _id = Id("alpha"))
      val bravo = Parent("Bravo", _id = Id("bravo"))

      for
        _ <- DB.init
        _ <- DB.truncate()
        _ <- DB.parents.transaction(_.insert(List(alpha, bravo)))
        _ <- DB.children.transaction(_.insert(List(
          Child(alpha._id, state = Some("WY")),
          Child(bravo._id, state = Some("UT"))
        )))
        ids <- DB.parents.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(1)
            .filter(_.childFilter(_.state === Some("WY")))
            .id
            .toList
        }
      yield {
        ids shouldBe List(alpha._id)
      }
    }

    "early-terminate ExistsChild (multi) where different children satisfy different clauses" in {
      val alpha = Parent("Alpha", _id = Id("alpha2"))
      val bravo = Parent("Bravo", _id = Id("bravo2"))

      val kids = List(
        Child(alpha._id, state = Some("WY")),
        Child(alpha._id, range = Some("68W")),
        Child(bravo._id, state = Some("WY"))
      )

      for
        _ <- DB.truncate()
        _ <- DB.parents.transaction(_.insert(List(alpha, bravo)))
        _ <- DB.children.transaction(_.insert(kids))
        ids <- DB.parents.transaction { tx =>
          tx.query
            .clearPageSize
            .limit(10)
            .filter(p => p.childFilter(_.state === Some("WY")) && p.childFilter(_.range === Some("68W")))
            .sort(Sort.IndexOrder)
            .id
            .toList
        }
      yield {
        ids.toSet shouldBe Set(alpha._id)
      }
    }
  }
}


