package spec

import fabric.rw.*
import lightdb.doc.{JsonConversion, ParentChildSupport, RecordDocument, RecordDocumentModel}
import lightdb.field.Field
import lightdb.filter.FilterExtras
import lightdb.id.Id
import lightdb.store.Collection
import lightdb.time.Timestamp
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

abstract class AbstractParentChildSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName

  protected def parents: Collection[Parent, Parent.type]
  protected def children: Collection[Child, Child.type]
  protected def initDb(): Task[Unit]
  protected def truncateDb(): Task[Unit]
  protected def disposeDb(): Task[Unit]
  protected def indexParentsAndChildren(parents: List[Parent], children: List[Child]): Task[Unit]

  private val alpha = Parent(name = "Alpha")
  private val bravo = Parent(name = "Bravo")
  private val charlie = Parent(name = "Charlie")
  private val delta = Parent(name = "Delta")
  private val echo = Parent(name = "Echo")

  private val childrenData = List(
    Child(parentId = alpha._id, state = Some("WY")),
    Child(parentId = alpha._id, range = Some("68W")),
    Child(parentId = bravo._id, state = Some("WY"), range = Some("69W")),
    Child(parentId = charlie._id, range = Some("68W")),
    Child(parentId = delta._id, state = Some("UT")),
    Child(parentId = delta._id, range = Some("70W")),
    Child(parentId = echo._id, state = Some("UT"), range = Some("70W"))
  )

  private def byParent(id: Id[Parent]): List[Child] = childrenData.filter(_.parentId == id)

  specName should {
    "initialize the database" in {
      initDb().succeed
    }
    "index parent and child data" in {
      val parentData = List(alpha, bravo, charlie, delta, echo)
      indexParentsAndChildren(parentData, childrenData).succeed
    }
    "match parents when a child satisfies a single condition" in {
      parents.transaction { tx =>
        tx.query
          .filter(_.childFilter(_.state === Some("WY")))
          .id
          .toList
          .map(_.toSet should be(Set(alpha._id, bravo._id)))
      }
    }
    "match parents when different children satisfy different conditions" in {
      parents.transaction { tx =>
        tx.query
          .filter(p => p.childFilter(_.state === Some("WY")) && p.childFilter(_.range === Some("68W")))
          .id
          .toList
          .map(_ should be(List(alpha._id)))
      }
    }
    "match parents when different children satisfy different conditions (collective helper)" in {
      parents.transaction { tx =>
        tx.query
          .filter(_.childFilterCollectiveAll(_.state === Some("WY"), _.range === Some("68W")))
          .id
          .toList
          .map(_ should be(List(alpha._id)))
      }
    }
    "match parents when both conditions are met on a single child" in {
      parents.transaction { tx =>
        tx.query
          .filter(p => p.childFilter(c => c.state === Some("UT") && c.range === Some("70W")))
          .toList
          .map(_.map(_._id).toSet should be(Set(echo._id)))
      }
    }
    "match parents when both conditions are met on a single child (same-child helper)" in {
      parents.transaction { tx =>
        tx.query
          .filter(_.childFilterSameAll(_.state === Some("UT"), _.range === Some("70W")))
          .toList
          .map(_.map(_._id).toSet should be(Set(echo._id)))
      }
    }
    "materialize parent documents from stored fields" in {
      parents.transaction { tx =>
        tx.query
          .filter(_.name === "Alpha")
          .toList
          .map { list =>
            list.map(_.name) should be(List("Alpha"))
          }
      }
    }
    "truncate and dispose the database" in {
      truncateDb().next(disposeDb()).succeed
    }
  }

  case class Parent(name: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Parent] = Parent.id()) extends RecordDocument[Parent]

  object Parent extends RecordDocumentModel[Parent] with JsonConversion[Parent] with ParentChildSupport[Parent, Child, Child.type] {
    override implicit val rw: RW[Parent] = RW.gen

    override def childStore: Collection[Child, Child.type] = children

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
