package spec

import fabric.rw._
import lightdb.doc.{DocumentModel, JsonConversion, RecordDocument, RecordDocumentModel}
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
  private val parentWithBoth = Parent(name = "Alpha", category = "Well")
  private val parentMailOnly = Parent(name = "Bravo", category = "Well")
  private val parentLegalOnly = Parent(name = "Charlie", category = "Lease")
  private val parentIrrelevant = Parent(name = "Delta", category = "Well")

  private val children = List(
    Child(parentId = parentWithBoth._id, kind = "MailingAddress", state = Some("WY")),
    Child(parentId = parentWithBoth._id, kind = "LegalDescription", range = Some("68W")),
    Child(parentId = parentMailOnly._id, kind = "MailingAddress", state = Some("WY")),
    Child(parentId = parentLegalOnly._id, kind = "LegalDescription", range = Some("68W")),
    Child(parentId = parentIrrelevant._id, kind = "MailingAddress", state = Some("UT"))
  )

  protected lazy val specName: String = getClass.getSimpleName

  protected val db: DB = new DB

  private val relation = ParentChildRelation[Parent, Child](db.children, _.parentId)

  private def mailingInWy(model: DocumentModel[Child]): Filter[Child] = {
    val m = model.asInstanceOf[Child.type]
    (m.kind === "MailingAddress") && (m.state === Some("WY"))
  }

  private def legalRange(model: DocumentModel[Child]): Filter[Child] = {
    val m = model.asInstanceOf[Child.type]
    (m.kind === "LegalDescription") && (m.range === Some("68W"))
  }

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "insert parents and children" in {
      for {
        _ <- db.parents.transaction(_.insert(List(parentWithBoth, parentMailOnly, parentLegalOnly, parentIrrelevant)))
        _ <- db.children.transaction(_.insert(children))
      } yield succeed
    }
    "match parents when a child satisfies a single condition" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_ => existsChild(relation)(mailingInWy))
          .id
          .toList
          .map(_.toSet should be(Set(parentWithBoth._id, parentMailOnly._id)))
      }
    }
    "match parents when different children satisfy different conditions" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_ => existsChild(relation)(mailingInWy) && existsChild(relation)(legalRange))
          .id
          .toList
          .map(_ should be(List(parentWithBoth._id)))
      }
    }
    "not match parents missing one of the required child conditions" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_ => existsChild(relation)(mailingInWy) && existsChild(relation)(legalRange))
          .sort(Sort.ByField(Parent.name))
          .toList
          .map(_.map(_._id).toSet should be(Set(parentWithBoth._id)))
      }
    }
    "combine parent and child filters" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_.category === "Well")
          .filter(_ => existsChild(relation)(mailingInWy) && existsChild(relation)(legalRange))
          .id
          .toList
          .map(_ should be(List(parentWithBoth._id)))
      }
    }
    "match none when no children satisfy the condition" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_ => existsChild(relation)(_ => Child.kind === "NonExistentKind"))
          .toList
          .map(_ shouldBe empty)
      }
    }
    "truncate and dispose the database" in {
      db.truncate().flatMap(_ => db.dispose).succeed
    }
  }

  def storeManager: CollectionManager

  class DB extends LightDB {
    override type SM = CollectionManager
    override val storeManager: CollectionManager = spec.storeManager

    override def name: String = specName
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val parents: Collection[Parent, Parent.type] = store(Parent)
    val children: Collection[Child, Child.type] = store(Child)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Parent(name: String,
                    category: String,
                    created: Timestamp = Timestamp(),
                    modified: Timestamp = Timestamp(),
                    _id: Id[Parent] = Parent.id()) extends RecordDocument[Parent]
  object Parent extends RecordDocumentModel[Parent] with JsonConversion[Parent] {
    override implicit val rw: RW[Parent] = RW.gen

    val name: I[String] = field.index(_.name)
    val category: I[String] = field.index(_.category)
  }

  case class Child(parentId: Id[Parent],
                   kind: String,
                   state: Option[String] = None,
                   range: Option[String] = None,
                   created: Timestamp = Timestamp(),
                   modified: Timestamp = Timestamp(),
                   _id: Id[Child] = Child.id()) extends RecordDocument[Child]
  object Child extends RecordDocumentModel[Child] with JsonConversion[Child] {
    override implicit val rw: RW[Child] = RW.gen

    val parentId: I[Id[Parent]] = field.index(_.parentId)
    val kind: I[String] = field.index(_.kind)
    val state: I[Option[String]] = field.index(_.state)
    val range: I[Option[String]] = field.index(_.range)
  }
}

