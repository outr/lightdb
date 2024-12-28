package spec

import fabric.rw._
import lightdb.collection.Collection
import lightdb.{Id, LightDB}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.Path

abstract class AbstractKeyValueSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers { spec =>
  val CreateRecords = 100_000

  private val adam = User("Adam", 21, _id = User.id("adam"))
  private val brenda = User("Brenda", 11, _id = User.id("brenda"))
  private val charlie = User("Charlie", 35, _id = User.id("charlie"))
  private val diana = User("Diana", 15, _id = User.id("diana"))
  private val evan = User("Evan", 53, _id = User.id("evan"))
  private val fiona = User("Fiona", 23, _id = User.id("fiona"))
  private val greg = User("Greg", 12, _id = User.id("greg"))
  private val hanna = User("Hanna", 62, _id = User.id("hanna"))
  private val ian = User("Ian", 89, _id = User.id("ian"))
  private val jenna = User("Jenna", 4, _id = User.id("jenna"))
  private val kevin = User("Kevin", 33, _id = User.id("kevin"))
  private val linda = User("Linda", 72, _id = User.id("linda"))
  private val mike = User("Mike", 42, _id = User.id("mike"))
  private val nancy = User("Nancy", 22, _id = User.id("nancy"))
  private val oscar = User("Oscar", 21, _id = User.id("oscar"))
  private val penny = User("Penny", 2, _id = User.id("penny"))
  private val quintin = User("Quintin", 99, _id = User.id("quintin"))
  private val ruth = User("Ruth", 102, _id = User.id("ruth"))
  private val sam = User("Sam", 81, _id = User.id("sam"))
  private val tori = User("Tori", 30, _id = User.id("tori"))
  private val uba = User("Uba", 21, _id = User.id("uba"))
  private val veronica = User("Veronica", 13, _id = User.id("veronica"))
  private val wyatt = User("Wyatt", 30, _id = User.id("wyatt"))
  private val xena = User("Xena", 63, _id = User.id("xena"))
  private val yuri = User("Yuri", 30, _id = User.id("yuri"))
  private val zoey = User("Zoey", 101, _id = User.id("zoey"))

  private val names = List(
    adam, brenda, charlie, diana, evan, fiona, greg, hanna, ian, jenna, kevin, linda, mike, nancy, oscar, penny,
    quintin, ruth, sam, tori, uba, veronica, wyatt, xena, yuri, zoey
  )

  protected lazy val specName: String = getClass.getSimpleName
  
  protected var db: DB = new DB

  specName should {
    "initialize the database" in {
      db.init().map(_ should be(true))
    }
    "verify the database is empty" in {
      db.users.transaction { implicit transaction =>
        db.users.count.map(_ should be(0))
      }
    }
    "insert the records" in {
      db.users.transaction { implicit transaction =>
        db.users.insert(names).map(_ should not be None)
      }
    }
    "retrieve the first record by _id -> id" in {
      db.users.transaction { implicit transaction =>
        db.users(_._id -> adam._id).map(_ should be(adam))
      }
    }
    "retrieve the first record by id" in {
      db.users.transaction { implicit transaction =>
        db.users(adam._id).map(_ should be(adam))
      }
    }
    "count the records in the database" in {
      db.users.transaction { implicit transaction =>
        db.users.count.map(_ should be(26))
      }
    }
    "stream the records in the database" in {
      db.users.transaction { implicit transaction =>
        db.users.stream.map(_.age).toList.map(_.toSet).map { ages =>
          ages should be(Set(101, 42, 89, 102, 53, 13, 2, 22, 12, 81, 35, 63, 99, 23, 30, 4, 21, 33, 11, 72, 15, 62))
        }
      }
    }
    "delete some records" in {
      db.users.transaction { implicit transaction =>
        for {
          d1 <- db.users.delete(_._id -> linda._id)
          d2 <- db.users.delete(_._id -> yuri._id)
        } yield {
          d1 should be(true)
          d2 should be(true)
        }
      }
    }
    "verify the records were deleted" in {
      db.users.transaction { implicit transaction =>
        db.users.count.map(_ should be(24))
      }
    }
    "modify a record" in {
      db.users.transaction { implicit transaction =>
        db.users.modify(adam._id) {
          case Some(p) => Task.pure(Some(p.copy(name = "Allan")))
          case None => fail("Adam was not found!")
        }
      }.map {
        case Some(p) => p.name should be("Allan")
        case None => fail("Allan was not returned!")
      }
    }
    "verify the record has been renamed" in {
      db.users.transaction { implicit transaction =>
        db.users(_._id -> adam._id).map(_.name should be("Allan"))
      }
    }
    "insert a lot more names" in {
      db.users.transaction { implicit transaction =>
        val p = (1 to CreateRecords).toList.map { index =>
          User(
            name = s"Unique Snowflake $index",
            age = if (index > 100) 0 else index,
          )
        }
        db.users.insert(p).succeed
      }
    }
    "verify the correct number of people exist in the database" in {
      db.users.transaction { implicit transaction =>
        db.users.count.map(_ should be(CreateRecords + 24))
      }
    }
    "truncate the collection again" in {
      db.users.transaction { implicit transaction =>
        db.users.truncate().map(_ should be(CreateRecords + 24))
      }
    }
    "dispose the database" in {
      db.dispose().succeed
    }
  }

  def storeManager: StoreManager

  class DB extends LightDB {
    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val users: Collection[User, User.type] = collection(User)

    override def storeManager: StoreManager = spec.storeManager

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class User(name: String, age: Int, _id: Id[User] = User.id()) extends Document[User]

  object User extends DocumentModel[User] with JsonConversion[User] {
    override implicit val rw: RW[User] = RW.gen

    val name: I[String] = field.index("name", _.name)
    val age: F[Int] = field("age", _.age)
  }
}
