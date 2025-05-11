package spec

import fabric.rw._
import lightdb._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.id.{EdgeId, Id}
import lightdb.store.{PrefixScanningStoreManager, Store, StoreManager}
import lightdb.traversal._
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

abstract class AbstractDeliveryPathSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName
  protected lazy val db: DB = new DB

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "insert delivery chain nodes" in {
      db.transactions(db.warehouses, db.trucks, db.depots, db.drones, db.customers) {
        case (warehouses, trucks, depots, drones, customers) =>
          for {
            _ <- warehouses.insert(Warehouse("Warehouse 1", Id("warehouse1")))
            _ <- trucks.insert(Truck("Truck 1", Id("truck1")))
            _ <- depots.insert(Depot("Depot 1", Id("depot1")))
            _ <- drones.insert(Drone("Drone 1", Id("drone1")))
            _ <- customers.insert(Customer("Customer 1", Id("customer1")))
          } yield succeed
      }
    }
    "insert delivery chain edges" in {
      db.transactions(db.shipsTo, db.deliversToDepot, db.loadsTo, db.deliversToCustomer) {
        case (shipsTo, deliversToDepot, loadsTo, deliversToCustomer) =>
          for {
            _ <- shipsTo.insert(ShipsTo(Id("warehouse1"), Id("truck1")))
            _ <- deliversToDepot.insert(DeliversToDepot(Id("truck1"), Id("depot1")))
            _ <- loadsTo.insert(LoadsTo(Id("depot1"), Id("drone1")))
            _ <- deliversToCustomer.insert(DeliversToCustomer(Id("drone1"), Id("customer1")))
          } yield succeed
      }
    }
    "traverse full delivery path from warehouse to customer" in {
      db.transactions(
        db.shipsTo,
        db.deliversToDepot,
        db.loadsTo,
        db.deliversToCustomer,
        db.customers
      ) {
        case (shipsTo, deliversToDepot, loadsTo, deliversToCustomer, customers) =>
          shipsTo.traversal.edgesFor(Id[Warehouse]("warehouse1"))
            .flatMap { s =>
              deliversToDepot.traversal.edgesFor(s._to)
            }
            .flatMap { d =>
              loadsTo.traversal.edgesFor(d._to)
            }
            .flatMap { d =>
              deliversToCustomer.traversal.edgesFor(d._to)
            }
            .evalMap(dtc => customers(dtc._to))
            .toList
            .map { customers =>
              customers.map(_.name) should be(List("Customer 1"))
            }
//          for {
//            customers <- shipsTo.traverse(Id[Warehouse]("warehouse1"))
//              .step(GraphStep.forward[DeliversToDepot, DeliversToDepot.type, Truck, Depot](DeliversToDepot))(deliversToDepot)
//              .step(GraphStep.forward[LoadsTo, LoadsTo.type, Depot, Drone](LoadsTo))(loadsTo)
//              .step(GraphStep.forward[DeliversToCustomer, DeliversToCustomer.type, Drone, Customer](DeliversToCustomer))(deliversToCustomer)
//              .collectAllReachable()
//          } yield customers should contain only Id("customer1")
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
  def storeManager: PrefixScanningStoreManager

  class DB extends LightDB {
    override type SM = PrefixScanningStoreManager
    override val storeManager: SM = spec.storeManager

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    override def upgrades: List[DatabaseUpgrade] = Nil

    val warehouses: S[Warehouse, WarehouseModel.type] = store[Warehouse, WarehouseModel.type](WarehouseModel)
    val trucks: S[Truck, TruckModel.type] = store[Truck, TruckModel.type](TruckModel)
    val depots: S[Depot, DepotModel.type] = store[Depot, DepotModel.type](DepotModel)
    val drones: S[Drone, DroneModel.type] = store[Drone, DroneModel.type](DroneModel)
    val customers: S[Customer, CustomerModel.type] = store[Customer, CustomerModel.type](CustomerModel)

    val shipsTo: S[ShipsTo, ShipsTo.type] = store[ShipsTo, ShipsTo.type](ShipsTo)
    val deliversToDepot: S[DeliversToDepot, DeliversToDepot.type] = store[DeliversToDepot, DeliversToDepot.type](DeliversToDepot)
    val loadsTo: S[LoadsTo, LoadsTo.type] = store[LoadsTo, LoadsTo.type](LoadsTo)
    val deliversToCustomer: S[DeliversToCustomer, DeliversToCustomer.type] = store[DeliversToCustomer, DeliversToCustomer.type](DeliversToCustomer)
  }
}

case class Warehouse(name: String, _id: Id[Warehouse] = Id()) extends Document[Warehouse]
object WarehouseModel extends DocumentModel[Warehouse] with JsonConversion[Warehouse] {
  override implicit val rw: RW[Warehouse] = RW.gen

  val name: F[String] = field("name", _.name)
}

case class Truck(name: String, _id: Id[Truck] = Id()) extends Document[Truck]
object TruckModel extends DocumentModel[Truck] with JsonConversion[Truck] {
  override implicit val rw: RW[Truck] = RW.gen

  val name: F[String] = field("name", _.name)
}

case class Depot(name: String, _id: Id[Depot] = Id()) extends Document[Depot]
object DepotModel extends DocumentModel[Depot] with JsonConversion[Depot] {
  override implicit val rw: RW[Depot] = RW.gen

  val name: F[String] = field("name", _.name)
}

case class Drone(name: String, _id: Id[Drone] = Id()) extends Document[Drone]
object DroneModel extends DocumentModel[Drone] with JsonConversion[Drone] {
  override implicit val rw: RW[Drone] = RW.gen

  val name: F[String] = field("name", _.name)
}

case class Customer(name: String, _id: Id[Customer] = Id()) extends Document[Customer]
object CustomerModel extends DocumentModel[Customer] with JsonConversion[Customer] {
  override implicit val rw: RW[Customer] = RW.gen

  val name: F[String] = field("name", _.name)
}

case class ShipsTo(_from: Id[Warehouse], _to: Id[Truck], _id: EdgeId[ShipsTo, Warehouse, Truck]) extends EdgeDocument[ShipsTo, Warehouse, Truck] with Document[ShipsTo]
object ShipsTo extends EdgeModel[ShipsTo, Warehouse, Truck] with JsonConversion[ShipsTo] {
  override implicit val rw: RW[ShipsTo] = RW.gen

  def apply(_from: Id[Warehouse], _to: Id[Truck]): ShipsTo = ShipsTo(_from, _to, EdgeId(_from, _to))
}

case class DeliversToDepot(_from: Id[Truck], _to: Id[Depot], _id: EdgeId[DeliversToDepot, Truck, Depot]) extends EdgeDocument[DeliversToDepot, Truck, Depot] with Document[DeliversToDepot]
object DeliversToDepot extends EdgeModel[DeliversToDepot, Truck, Depot] with JsonConversion[DeliversToDepot] {
  override implicit val rw: RW[DeliversToDepot] = RW.gen

  def apply(_from: Id[Truck], _to: Id[Depot]): DeliversToDepot = DeliversToDepot(_from, _to, EdgeId(_from, _to))
}

case class LoadsTo(_from: Id[Depot], _to: Id[Drone], _id: EdgeId[LoadsTo, Depot, Drone]) extends EdgeDocument[LoadsTo, Depot, Drone] with Document[LoadsTo]
object LoadsTo extends EdgeModel[LoadsTo, Depot, Drone] with JsonConversion[LoadsTo] {
  override implicit val rw: RW[LoadsTo] = RW.gen

  def apply(_from: Id[Depot], _to: Id[Drone]): LoadsTo = LoadsTo(_from, _to, EdgeId(_from, _to))
}

case class DeliversToCustomer(_from: Id[Drone], _to: Id[Customer], _id: EdgeId[DeliversToCustomer, Drone, Customer]) extends EdgeDocument[DeliversToCustomer, Drone, Customer] with Document[DeliversToCustomer]
object DeliversToCustomer extends EdgeModel[DeliversToCustomer, Drone, Customer] with JsonConversion[DeliversToCustomer] {
  override implicit val rw: RW[DeliversToCustomer] = RW.gen

  def apply(_from: Id[Drone], _to: Id[Customer]): DeliversToCustomer = DeliversToCustomer(_from, _to, EdgeId(_from, _to))
}