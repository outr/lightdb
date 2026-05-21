package spec

import fabric.*
import fabric.io.JsonParser
import fabric.rw.*
import lightdb.LightDB
import lightdb.api.{ApiContent, ApiRequest, ApiResponse, LightDBHttpHandler, Operation}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.{Store, StoreManager}
import lightdb.store.hashmap.HashMapStore
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path}
import java.util.Comparator

@EmbeddedTest
class LightDBHttpHandlerSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName
  private lazy val dbPath: Path = Path.of(s"db/$specName")

  deleteDirectoryIfExists(dbPath)

  private val db = new TestDB
  private val handler: LightDBHttpHandler = new LightDBHttpHandler {
    override def db: LightDB = spec.db
  }

  private val adam = User("Adam", 21, _id = Id[User]("adam"))
  private val brenda = User("Brenda", 11, _id = Id[User]("brenda"))
  private val charlie = User("Charlie", 35, _id = Id[User]("charlie"))

  specName should {
    "initialize the database" in {
      db.init.next(db.users.transaction(_.insert(List(adam, brenda, charlie)))).succeed
    }

    "list stores" in {
      handler.handle(ApiRequest("GET", "/stores")).map { resp =>
        resp.status shouldBe 200
        val j = jsonOf(resp)
        val names = j("stores").asVector.map(_("name").asString).toSet
        names should contain("User")
      }
    }

    "count documents" in {
      handler.handle(ApiRequest("GET", "/stores/User/count")).map { resp =>
        resp.status shouldBe 200
        jsonOf(resp)("count").asInt shouldBe 3
      }
    }

    "fetch document by id" in {
      handler.handle(ApiRequest("GET", "/stores/User/adam")).map { resp =>
        resp.status shouldBe 200
        jsonOf(resp)("name").asString shouldBe "Adam"
      }
    }

    "404 on missing id" in {
      handler.handle(ApiRequest("GET", "/stores/User/missing")).map { resp =>
        resp.status shouldBe 404
      }
    }

    "404 on unknown store" in {
      handler.handle(ApiRequest("GET", "/stores/nope/count")).map { resp =>
        resp.status shouldBe 404
      }
    }

    "list documents with paging" in {
      handler.handle(ApiRequest(
        method = "GET",
        path = "/stores/User",
        params = Map("limit" -> "2", "offset" -> "0")
      )).map { resp =>
        resp.status shouldBe 200
        val j = jsonOf(resp)
        j("limit").asInt shouldBe 2
        j("docs").asVector.length shouldBe 2
      }
    }

    "respect maxPageSize clamp" in {
      val clampedHandler = new LightDBHttpHandler {
        override def db: LightDB = spec.db
        override def maxPageSize: Int = 2
      }
      val op = clampedHandler.parse(ApiRequest(
        method = "GET", path = "/stores/User", params = Map("limit" -> "500")
      ))
      op match {
        case Operation.ListDocs(_, limit, _) => Task.pure(limit shouldBe 2)
        case other => Task.pure(fail(s"Expected ListDocs, got $other"))
      }
    }

    "upsert a document" in {
      val newUser = User("Dora", 9, _id = Id[User]("dora"))
      val body = newUser.json(User.rw)
      handler.handle(ApiRequest("POST", "/stores/User", body = Some(body))).flatMap { resp =>
        resp.status shouldBe 200
        handler.handle(ApiRequest("GET", "/stores/User/dora")).map { fetched =>
          fetched.status shouldBe 200
          jsonOf(fetched)("name").asString shouldBe "Dora"
        }
      }
    }

    "reject malformed upsert body" in {
      handler.handle(ApiRequest("POST", "/stores/User", body = Some(obj("nope" -> str("x"))))).map { resp =>
        resp.status shouldBe 400
      }
    }

    "stream documents as SSE content" in {
      handler.handle(ApiRequest("GET", "/stores/User/stream")).flatMap { resp =>
        resp.status shouldBe 200
        resp.content match {
          case ApiContent.Stream(events) =>
            events.toList.map { docs =>
              docs.length should be >= 4
              docs.forall(j => j("_id").asString.nonEmpty) shouldBe true
            }
          case other => Task.pure(fail(s"Expected SSE stream, got $other"))
        }
      }
    }

    "delete a document" in {
      handler.handle(ApiRequest("DELETE", "/stores/User/dora")).flatMap { resp =>
        resp.status shouldBe 200
        handler.handle(ApiRequest("GET", "/stores/User/dora")).map { fetched =>
          fetched.status shouldBe 404
        }
      }
    }

    "404 on delete of missing id" in {
      handler.handle(ApiRequest("DELETE", "/stores/User/missing")).map { resp =>
        resp.status shouldBe 404
      }
    }

    "truncate the store" in {
      handler.handle(ApiRequest("POST", "/stores/User/truncate")).flatMap { resp =>
        resp.status shouldBe 200
        handler.handle(ApiRequest("GET", "/stores/User/count")).map { c =>
          jsonOf(c)("count").asInt shouldBe 0
        }
      }
    }

    "404 on unknown route" in {
      handler.handle(ApiRequest("GET", "/no/such/thing")).map { resp =>
        resp.status shouldBe 404
      }
    }

    "dispose the database" in {
      db.dispose.succeed
    }
  }

  private def jsonOf(resp: ApiResponse): Json = resp.content match {
    case ApiContent.JsonValue(v) => v
    case other                   => throw new RuntimeException(s"Expected JSON content, got $other")
  }

  private def deleteDirectoryIfExists(path: Path): Unit = if Files.exists(path) then {
    Files.walk(path).sorted(Comparator.reverseOrder()).forEach(Files.delete(_))
  }

  case class User(name: String, age: Int, _id: Id[User] = Id[User]()) extends Document[User]
  object User extends DocumentModel[User] with JsonConversion[User] {
    override implicit val rw: RW[User] = RW.gen
    val name: I[String] = field.index("name", _.name)
    val age: F[Int] = field("age", _.age)
  }

  class TestDB extends LightDB {
    override type SM = StoreManager
    override val storeManager: StoreManager = HashMapStore
    val directory: Option[Path] = Some(dbPath)
    val users: Store[User, User.type] = store(User)()
    override def upgrades: List[DatabaseUpgrade] = Nil
  }
}
