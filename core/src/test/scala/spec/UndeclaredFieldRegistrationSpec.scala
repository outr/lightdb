package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.hashmap.HashMapStore
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.*

import java.nio.file.Path
import java.util.concurrent.{CyclicBarrier, TimeUnit}

/**
 * Initializing a store registers on its model every document field the model does not declare. A model shared by
 * several stores must end up with each such field registered exactly once, whether the stores initialize one after the
 * other or at the same time.
 */
@EmbeddedTest
class UndeclaredFieldRegistrationSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  import UndeclaredFieldRegistrationSpec.*

  private val StoresPerModel = 8
  private val Rounds = 25

  private def fieldNames(model: ItemModel): List[String] = model.fields.map(_.name)

  "Undeclared fields" should {
    "be registered once when stores of one model initialize concurrently" in {
      val rounds = (1 to Rounds).toList.map { round =>
        val model = new ItemModel(StoresPerModel)
        val db = new DB(s"round$round")
        val stores = (1 to StoresPerModel).toList.map(i => db.store(model).withName(s"items$i")())
        Task.parSequence(stores.map(_.init)).map(_ => fieldNames(model))
      }
      Task.parSequence(rounds).map { results =>
        results.foreach { names =>
          names.diff(names.distinct) shouldBe Nil
          names should contain allOf ("name", "count")
        }
        succeed
      }
    }
    "be registered once when stores of one model initialize one after the other" in {
      val model = new ItemModel(1)
      val db = new DB("sequential")
      val stores = (1 to 3).toList.map(i => db.store(model).withName(s"items$i")())
      stores.map(_.init).tasks.map { _ =>
        val names = fieldNames(model)
        names.diff(names.distinct) shouldBe Nil
        names should contain allOf ("name", "count")
      }
    }
  }
}

object UndeclaredFieldRegistrationSpec {
  case class Item(name: String, count: Int, _id: Id[Item]) extends Document[Item]

  /** Declares only `_id`. Its stores wait for each other before registering fields, so they register them together. */
  class ItemModel(stores: Int) extends DocumentModel[Item] with JsonConversion[Item] {
    override implicit val rw: RW[Item] = RW.gen

    private val barrier = new CyclicBarrier(stores)

    override protected def init[Model <: DocumentModel[Item]](store: Store[Item, Model]): Task[Unit] =
      Task(barrier.await(30, TimeUnit.SECONDS)).unit
  }

  class DB(dbName: String) extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = HashMapStore

    override def name: String = s"UndeclaredFieldRegistrationSpec-$dbName"

    lazy val directory: Option[Path] = None

    override protected def disposeOnShutdown: Boolean = false

    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
