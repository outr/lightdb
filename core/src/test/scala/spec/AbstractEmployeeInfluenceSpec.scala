package spec

import fabric.rw._
import lightdb._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.{EdgeDocument, EdgeModel, ReverseEdgeDocument}
import lightdb.id.{EdgeId, Id}
import lightdb.store.prefix.PrefixScanningStoreManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

abstract class AbstractEmployeeInfluenceSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName
  protected lazy val db: DB = new DB

  private val reports = List(
    ReportsTo(Id("bob"), Id("alice")), // Bob reports to Alice
    ReportsTo(Id("carol"), Id("bob"))  // Carol reports to Bob
  )

  private val subordinates = reports.map(rt => ReverseEdgeDocument[ReportsTo, Employee, Employee](rt))

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "insert employees" in {
      db.employees.transaction { tx =>
        tx.insert(List(
          Employee("Alice", Id("alice")),
          Employee("Bob", Id("bob")),
          Employee("Carol", Id("carol")),
          Employee("Dave", Id("dave"))
        ))
      }.succeed
    }
    "insert reports to edges" in {
      db.reportsTo.transaction { tx =>
        tx.insert(reports)
      }.succeed
    }
    "insert collaborates with edges" in {
      db.collaboratesWith.transaction { tx =>
        tx.insert(List(
          CollaboratesWith(Id("carol"), Id("dave")) // Carol collaborates with Dave
        ))
      }.succeed
    }
    "verify who alice reports to and collaborates with" in {
      db.collaboratesWith.transaction { collaboratesWith =>
        db.subordinates.transaction { subordinates =>
          def reportsAndCollaboratesStep: Id[Employee] => Task[Set[Id[Employee]]] = { id =>
            for {
              reports <- subordinates.traverse.edgesFor[ReverseEdgeDocument[ReportsTo, Employee, Employee], Employee, Employee](id).map(_._to).toList.map(_.toSet)
              collabs <- collaboratesWith.traverse.edgesFor[CollaboratesWith, Employee, Employee](id).map(_._to).toList.map(_.toSet)
            } yield reports ++ collabs
          }

          val start = Set(Id[Employee]("alice"))

          traverse
            .withStepFunction(start, reportsAndCollaboratesStep, maxDepth = 10)
            .collectAllReachable
            .map { results =>
              results should contain theSameElementsAs Set(Id("alice"), Id("bob"), Id("carol"), Id("dave"))
            }
        }
      }
    }
    "truncate the database" in {
      db.truncate().succeed
    }
    "dispose the database" in {
      db.dispose.next(dispose()).succeed
    }
  }

  def dispose(): Task[Unit] = Task.unit

  def storeManager: PrefixScanningStoreManager

  class DB extends LightDB {
    override type SM = PrefixScanningStoreManager
    override val storeManager: SM = spec.storeManager

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val employees: S[Employee, Employee.type] = store[Employee, Employee.type](Employee)
    val reportsTo: S[ReportsTo, ReportsTo.type] = store[ReportsTo, ReportsTo.type](ReportsTo)
    val collaboratesWith: S[CollaboratesWith, CollaboratesWith.type] = store[CollaboratesWith, CollaboratesWith.type](CollaboratesWith)

    val subordinatesModel: EdgeModel[ReverseEdgeDocument[ReportsTo, Employee, Employee], Employee, Employee] = ReverseEdgeDocument.createModel[ReportsTo, Employee, Employee]("subordinates")
    val subordinates: S[ReverseEdgeDocument[ReportsTo, Employee, Employee], subordinatesModel.type] = reverseStore[ReportsTo, Employee, Employee, ReportsTo.type, subordinatesModel.type](subordinatesModel, reportsTo)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}

case class Employee(name: String, _id: Id[Employee] = Id()) extends Document[Employee]

object Employee extends DocumentModel[Employee] with JsonConversion[Employee] {
  override implicit val rw: RW[Employee] = RW.gen

  val name: F[String] = field("name", _.name)
}

case class ReportsTo(_from: Id[Employee], _to: Id[Employee], _id: EdgeId[ReportsTo, Employee, Employee])
  extends EdgeDocument[ReportsTo, Employee, Employee] with Document[ReportsTo]

object ReportsTo extends EdgeModel[ReportsTo, Employee, Employee] with JsonConversion[ReportsTo] {
  override implicit val rw: RW[ReportsTo] = RW.gen

  def apply(_from: Id[Employee], _to: Id[Employee]): ReportsTo = ReportsTo(_from, _to, EdgeId(_from, _to))
}

case class CollaboratesWith(_from: Id[Employee], _to: Id[Employee], _id: EdgeId[CollaboratesWith, Employee, Employee])
  extends EdgeDocument[CollaboratesWith, Employee, Employee]

object CollaboratesWith extends EdgeModel[CollaboratesWith, Employee, Employee] with JsonConversion[CollaboratesWith] {
  override implicit val rw: RW[CollaboratesWith] = RW.gen

  def apply(_from: Id[Employee], _to: Id[Employee]): CollaboratesWith = CollaboratesWith(_from, _to, EdgeId(_from, _to))
}