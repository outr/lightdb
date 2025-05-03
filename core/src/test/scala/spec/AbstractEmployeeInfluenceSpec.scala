package spec

import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb._
import lightdb.store.{Store, StoreManager}
import lightdb.transaction.Transaction
import lightdb.traversal._
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

abstract class AbstractEmployeeInfluenceSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName
  protected lazy val db: DB = new DB

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "insert employees" in {
      db.employees.transaction { implicit tx =>
        tx.insert(List(
          Employee("Alice", Id("alice")),
          Employee("Bob", Id("bob")),
          Employee("Carol", Id("carol")),
          Employee("Dave", Id("dave"))
        ))
      }.succeed
    }
    "insert reports to edges" in {
      db.reportsTo.transaction { implicit tx =>
        tx.insert(List(
          ReportsTo(Id("bob"), Id("alice")), // Bob reports to Alice
          ReportsTo(Id("carol"), Id("bob"))  // Carol reports to Bob
        ))
      }.succeed
    }
    "insert collaborates with edges" in {
      db.collaboratesWith.transaction { implicit tx =>
        tx.insert(List(
          CollaboratesWith(Id("carol"), Id("dave")) // Carol collaborates with Dave
        ))
      }.succeed
    }
    "verify who alice reports to and collaborates with" in {
      db.reportsTo.transaction { implicit tx =>
        db.collaboratesWith.transaction { implicit ct =>
          db.reportsTo.traverse(Set(Id[Employee]("alice")))
            .bfs(ReportsAndCollaborationStep(db.collaboratesWith))
            .collectAllReachable()
            .map { results =>
              results should contain theSameElementsAs Set(Id("alice"), Id("bob"), Id("carol"), Id("dave"))
            }
        }
      }.succeed
    }
    "truncate the database" in {
      db.truncate().succeed
    }
    "dispose the database" in {
      db.dispose.next(dispose()).succeed
    }
  }

  def dispose(): Task[Unit] = Task.unit

  def storeManager: StoreManager

  class DB extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = spec.storeManager

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val employees: Store[Employee, EmployeeModel.type] = store(EmployeeModel)
    val reportsTo: Store[ReportsTo, ReportsToModel.type] = store(ReportsToModel)
    val collaboratesWith: Store[CollaboratesWith, CollaboratesWithModel.type] = store(CollaboratesWithModel)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}

case class Employee(name: String, _id: Id[Employee] = Id()) extends Document[Employee]

object EmployeeModel extends DocumentModel[Employee] with JsonConversion[Employee] {
  override implicit val rw: RW[Employee] = RW.gen

  val name: F[String] = field("name", _.name)
}

case class ReportsTo(_from: Id[Employee], _to: Id[Employee], _id: Id[ReportsTo] = Id())
  extends EdgeDocument[ReportsTo, Employee, Employee] with Document[ReportsTo]

object ReportsToModel extends EdgeModel[ReportsTo, Employee, Employee] with JsonConversion[ReportsTo] {
  override implicit val rw: RW[ReportsTo] = RW.gen
}

case class CollaboratesWith(_from: Id[Employee], _to: Id[Employee], _id: Id[CollaboratesWith] = Id())
  extends EdgeDocument[CollaboratesWith, Employee, Employee]

object CollaboratesWithModel extends EdgeModel[CollaboratesWith, Employee, Employee] with JsonConversion[CollaboratesWith] {
  override implicit val rw: RW[CollaboratesWith] = RW.gen
}

case class ReportsAndCollaborationStep(collaboratesWith: Store[CollaboratesWith, CollaboratesWithModel.type]) extends GraphStep[CollaboratesWith, CollaboratesWithModel.type, Employee, Employee] {
  override def neighbors(id: Id[Employee])(implicit tx: Transaction[CollaboratesWith, CollaboratesWithModel.type]): Task[Set[Id[Employee]]] = {
    for {
      subordinates <- ReportsToModel.reverseEdgesFor(id) // people who report to this ID
      collabs <- collaboratesWith.model.edgesFor(id)  // people this person collaborates with
    } yield subordinates ++ collabs
  }
}