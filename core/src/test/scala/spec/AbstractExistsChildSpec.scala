package spec

import fabric.rw._
import lightdb.doc.{JsonConversion, RecordDocument, RecordDocumentModel}
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
  private val alpha = Parent(name = "Alpha", category = "Well")
  private val bravo = Parent(name = "Bravo", category = "Well")
  private val charlie = Parent(name = "Charlie", category = "Lease")
  private val delta = Parent(name = "Delta", category = "Well")

  private val children = List(
    Child(parentId = alpha._id, kind = "MailingAddress", state = Some("WY")),
    Child(parentId = alpha._id, kind = "LegalDescription", range = Some("68W")),
    Child(parentId = bravo._id, kind = "MailingAddress", state = Some("WY")),
    Child(parentId = charlie._id, kind = "LegalDescription", range = Some("68W")),
    Child(parentId = delta._id, kind = "MailingAddress", state = Some("UT"))
  )

  protected lazy val specName: String = getClass.getSimpleName

  protected val db: DB = new DB

  private val relation = ParentChildRelation[Parent, Child, Child.type](db.children, _.parentId)

  private def mailingInWy(model: Child.type): Filter[Child] =
    (model.kind === "MailingAddress") && (model.state === Some("WY"))

  private def legalRange(model: Child.type): Filter[Child] =
    (model.kind === "LegalDescription") && (model.range === Some("68W"))

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "insert parents and children" in {
      for {
        _ <- db.parents.transaction(_.insert(List(alpha, bravo, charlie, delta)))
        _ <- db.children.transaction(_.insert(children))
      } yield succeed
    }
    "match parents when a child satisfies a single condition" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_ => existsChild(relation)(mailingInWy))
          .id
          .toList
          .map(_.toSet should be(Set(alpha._id, bravo._id)))
      }
    }
    "match parents when different children satisfy different conditions" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_ => existsChild(relation)(mailingInWy) && existsChild(relation)(legalRange))
          .id
          .toList
          .map(_ should be(List(alpha._id)))
      }
    }
    "not match parents missing one of the required child conditions" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_ => existsChild(relation)(mailingInWy) && existsChild(relation)(legalRange))
          .sort(Sort.ByField(Parent.name))
          .toList
          .map(_.map(_._id).toSet should be(Set(alpha._id)))
      }
    }
    "combine parent and child filters" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_.category === "Well")
          .filter(_ => existsChild(relation)(mailingInWy) && existsChild(relation)(legalRange))
          .id
          .toList
          .map(_ should be(List(alpha._id)))
      }
    }
    "match none when no children satisfy the condition" in {
      db.parents.transaction { tx =>
        tx.query
          .filter(_ => existsChild(relation)((_: Child.type) => Child.kind === "NonExistentKind"))
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

