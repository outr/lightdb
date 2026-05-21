package spec

import fabric.*
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.*
import lightdb.LightDB
import lightdb.api.{ApiContent, ApiRequest, LightDBHttpHandler}
import lightdb.api.spice.SpiceLightDBHandler
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.{Store, StoreManager}
import lightdb.store.hashmap.HashMapStore
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}
import spice.http.HttpMethod
import spice.http.client.HttpClient
import spice.http.content.{Content, StringContent}
import spice.http.server.MutableHttpServer
import spice.http.server.config.HttpServerListener
import spice.net.{ContentType, URL}

import java.nio.file.{Files, Path}
import java.util.Comparator

@EmbeddedTest
class SpiceLightDBHandlerSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  protected lazy val specName: String = getClass.getSimpleName
  private lazy val dbPath: Path = Path.of(s"db/$specName")

  deleteDirectoryIfExists(dbPath)

  private val db = new TestDB
  private val handler: LightDBHttpHandler = new LightDBHttpHandler {
    override def db: LightDB = spec.db
  }
  private val server = new MutableHttpServer
  // Bind to an ephemeral port: leaving `port = None` makes Undertow report the
  // OS-assigned port back into `config.listeners()` after start.
  server.config.clearListeners().addListeners(HttpServerListener(host = "127.0.0.1", port = None))
  SpiceLightDBHandler(handler).install(server)

  private def baseUrl: URL = {
    val l = server.config.listeners().collectFirst {
      case h: HttpServerListener => h
    }.getOrElse(throw new RuntimeException("No HTTP listener bound"))
    URL.parse(s"http://${l.host}:${l.port.getOrElse(0)}")
  }

  private val adam = User("Adam", 21, _id = Id[User]("adam"))

  specName should {
    "boot the server and seed the database" in {
      for
        _ <- db.init
        _ <- db.users.transaction(_.insert(adam))
        _ <- server.start()
      yield server.isRunning shouldBe true
    }

    "list stores via HTTP" in {
      get("/stores").map { (status, json) =>
        status shouldBe 200
        val names = json("stores").asVector.map(_("name").asString).toSet
        names should contain("User")
      }
    }

    "fetch a doc by id via HTTP" in {
      get("/stores/User/adam").map { (status, json) =>
        status shouldBe 200
        json("name").asString shouldBe "Adam"
        json("age").asInt shouldBe 21
      }
    }

    "upsert via HTTP POST" in {
      val newUser = User("Bea", 7, _id = Id[User]("bea"))
      post("/stores/User", newUser.json(User.rw)).flatMap { case (postStatus, _) =>
        postStatus shouldBe 200
        get("/stores/User/bea").map { case (getStatus, getBody) =>
          getStatus shouldBe 200
          getBody("name").asString shouldBe "Bea"
        }
      }
    }

    "count via HTTP" in {
      get("/stores/User/count").map { (status, json) =>
        status shouldBe 200
        json("count").asInt shouldBe 2
      }
    }

    "delete via HTTP" in {
      delete("/stores/User/bea").flatMap { (delStatus, _) =>
        delStatus shouldBe 200
        get("/stores/User/bea").map { (getStatus, _) =>
          getStatus shouldBe 404
        }
      }
    }

    "404 on unknown route" in {
      get("/no/such/thing").map { (status, _) =>
        status shouldBe 404
      }
    }

    "stop the server" in {
      for
        _ <- server.stop()
        _ <- db.dispose
      yield server.isRunning shouldBe false
    }
  }

  // -- HTTP helpers ----------------------------------------------------------

  private def get(path: String): Task[(Int, Json)] = HttpClient
    .url(baseUrl.withPath(spice.net.URLPath.parse(path)))
    .method(HttpMethod.Get)
    .noFailOnHttpStatus
    .send()
    .flatMap { resp =>
      bodyAsJson(resp.content).map(resp.status.code -> _)
    }

  private def post(path: String, body: Json): Task[(Int, Json)] = HttpClient
    .url(baseUrl.withPath(spice.net.URLPath.parse(path)))
    .method(HttpMethod.Post)
    .content(StringContent(JsonFormatter.Compact(body), ContentType.`application/json`))
    .noFailOnHttpStatus
    .send()
    .flatMap { resp =>
      bodyAsJson(resp.content).map(resp.status.code -> _)
    }

  private def delete(path: String): Task[(Int, Json)] = HttpClient
    .url(baseUrl.withPath(spice.net.URLPath.parse(path)))
    .method(HttpMethod.Delete)
    .noFailOnHttpStatus
    .send()
    .flatMap { resp =>
      bodyAsJson(resp.content).map(resp.status.code -> _)
    }

  private def bodyAsJson(content: Option[Content]): Task[Json] = content match {
    case Some(c) => c.asString.map { s =>
      if (s.trim.isEmpty) Null else JsonParser(s)
    }
    case None => Task.pure(Null)
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
