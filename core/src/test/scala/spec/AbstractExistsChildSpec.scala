package spec

import fabric.rw._
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.filter._
import lightdb.id.Id
import lightdb.store.{Collection, CollectionManager}
import lightdb.time.Timestamp
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{LightDB, Sort}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

abstract class AbstractExistsChildSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  private val alpha = Parent(name = "Alpha")
  private val bravo = Parent(name = "Bravo")
  private val charlie = Parent(name = "Charlie")
  private val delta = Parent(name = "Delta")
  private val echo = Parent(name = "Echo")

  private val children = List(
    Child(parentId = alpha._id, state = Some("WY")),
    Child(parentId = alpha._id, range = Some("68W")),
    Child(parentId = bravo._id, state = Some("WY"), range = Some("69W")),
    Child(parentId = charlie._id, range = Some("68W")),
    Child(parentId = delta._id, state = Some("UT")),
    Child(parentId = delta._id, range = Some("70W")),
    Child(parentId = echo._id, state = Some("UT"), range = Some("70W"))
  )

  protected lazy val specName: String = getClass.getSimpleName

  specName should {
    "initialize the database" in {
      DB.init.succeed
    }
    "insert parents and children" in {
      for {
        _ <- DB.parents.transaction(_.insert(List(alpha, bravo, charlie, delta, echo)))
        _ <- DB.children.transaction(_.insert(children))
      } yield succeed
    }
    "match parents when a child satisfies a single condition" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.state === Some("WY")))
          .id
          .toList
          .map(_.toSet should be(Set(alpha._id, bravo._id)))
      }
    }
    "match parents when different children satisfy different conditions" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(p => p.childFilter(_.state === Some("WY")) && p.childFilter(_.range === Some("68W")))
          .id
          .toList
          .map(_ should be(List(alpha._id)))
      }
    }
    "match parents when both conditions are met on a single child" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(p => p.childFilter(c => c.state === Some("UT") && c.range === Some("70W")))
          .toList
          .map(_.map(_._id).toSet should be(Set(echo._id)))
      }
    }
    "combine parent and child filters" in {
      DB.parents.transaction { tx =>
        tx.query
          .filter(_.name === "Alpha")
          .filter(p => p.childFilter(_.state === Some("WY")) && p.childFilter(_.range === Some("68W")))
          .id
          .toList
          .map(_ should be(List(alpha._id)))
      }
    }
    "truncate and dispose the database" in {
      DB.truncate().flatMap(_ => DB.dispose).succeed
    }
  }

  def storeManager: CollectionManager

  object DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager

    override def name: String = specName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

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

