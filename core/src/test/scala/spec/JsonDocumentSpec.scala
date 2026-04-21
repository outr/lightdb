package spec

import fabric.*
import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{JsonDocument, JsonDocumentModel}
import lightdb.store.{Store, StoreManager}
import lightdb.store.hashmap.HashMapStore
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

@EmbeddedTest
class JsonDocumentSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {

  val usersModel: JsonDocumentModel = JsonDocumentModel("users")
    .withField[String]("city")
    .withIndex[String]("name")
    .withIndex[Int]("age")

  class DB extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = HashMapStore
    lazy val directory: Option[Path] = None
    val users: Store[JsonDocument, JsonDocumentModel] = store(usersModel)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  private lazy val db = new DB

  "JsonDocument" should {
    "initialize the database" in {
      db.init.map(_ => db.isInitialized should be(true))
    }
    "insert and retrieve a document" in {
      db.users.transaction { tx =>
        val doc = JsonDocument(json = obj("name" -> str("Alice"), "age" -> num(30)))
        tx.insert(doc).flatMap { inserted =>
          tx(inserted._id).map { retrieved =>
            retrieved.json("name").asString should be("Alice")
            retrieved.json("age").asInt should be(30)
          }
        }
      }
    }
    "insert and retrieve a document with extra fields" in {
      db.users.transaction { tx =>
        val doc = JsonDocument(json = obj("name" -> str("Bob"), "age" -> num(25), "city" -> str("Portland")))
        tx.insert(doc).flatMap { inserted =>
          tx(inserted._id).map { retrieved =>
            retrieved.json("name").asString should be("Bob")
            retrieved.json("age").asInt should be(25)
            retrieved.json("city").asString should be("Portland")
          }
        }
      }
    }
    "create a products store after init" in {
      val productsModel = JsonDocumentModel("products")
        .withIndex[String]("sku")
        .withField[String]("description")
        .withIndex[Double]("price")

      val products = db.store(productsModel)()

      products.transaction { tx =>
        val doc = JsonDocument(json = obj(
          "sku" -> str("WIDGET-1"),
          "description" -> str("A fine widget"),
          "price" -> num(19.99)
        ))
        tx.insert(doc).flatMap { inserted =>
          tx(inserted._id).map { retrieved =>
            retrieved.json("sku").asString should be("WIDGET-1")
            retrieved.json("description").asString should be("A fine widget")
            retrieved.json("price").asDouble should be(19.99)
          }
        }
      }
    }
    "create a second ad-hoc store and use both concurrently" in {
      val eventsModel = JsonDocumentModel("events")
        .withIndex[String]("type")
        .withIndex[Long]("timestamp")

      val events = db.store(eventsModel)()

      for {
        _ <- events.transaction { tx =>
          val doc = JsonDocument(json = obj("type" -> str("click"), "timestamp" -> num(1709500000L)))
          tx.insert(doc).map(_ => ())
        }
        result <- events.transaction { tx =>
          tx.stream.toList.map { docs =>
            docs.size should be(1)
            docs.head.json("type").asString should be("click")
            docs.head.json("timestamp").asLong should be(1709500000L)
          }
        }
      } yield result
    }
    "dispose the database" in {
      db.dispose.map(_ => db.isDisposes should be(true))
    }
  }
}
