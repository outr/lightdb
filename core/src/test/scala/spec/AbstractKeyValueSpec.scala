package spec

import fabric.rw.*
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.{Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}

import java.nio.file.{Files, Path}
import java.util.Comparator

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

  private val adamAddress1 = Address(adam._id, "123 Eden Rd.")
  private val adamAddress2 = Address(adam._id, "321 Whipped St.")
  private val charlieAddress = Address(charlie._id, "Super Spins Blvd")

  private val names = List(
    adam, brenda, charlie, diana, evan, fiona, greg, hanna, ian, jenna, kevin, linda, mike, nancy, oscar, penny,
    quintin, ruth, sam, tori, uba, veronica, wyatt, xena, yuri, zoey
  )

  private val addresses = List(
    adamAddress1, adamAddress2, charlieAddress
  )

  protected lazy val specName: String = getClass.getSimpleName
  
  // Clear any leftover on-disk data before the first DB instance is created,
  // but leave subsequent instances intact so persistence checks remain valid.
  private lazy val dbPath: Path = Path.of(s"db/$specName")
  deleteDirectoryIfExists(dbPath)

  protected var _db: DB = _

  /**
   * Lazily create the DB instance so store-specific test helpers (e.g. config via Profig) can run during subclass
   * initialization before the first DB is constructed.
   */
  protected def db: DB = {
    if _db == null then {
      _db = new DB
    }
    _db
  }

  specName should {
    "initialize the database" in {
      db.init.succeed
    }
    "verify the database is empty" in {
      db.users.transaction { transaction =>
        transaction.count.map(_ should be(0))
      }
    }
    "insert the user records" in {
      db.users.transaction { transaction =>
        transaction.insert(names).map(_ should not be None)
      }
    }
    "insert the address records" in {
      db.addresses.transaction { transaction =>
        transaction.insert(addresses).map(_ should not be None)
      }
    }
    "retrieve the first record by _id -> id" in {
      db.users.transaction { transaction =>
        transaction(adam._id).map(_ should be(adam))
      }
    }
    "retrieve the first record by id" in {
      db.users.transaction { transaction =>
        transaction(adam._id).map(_ should be(adam))
      }
    }
    "count the records in the database" in {
      db.users.transaction { transaction =>
        transaction.count.map(_ should be(26))
      }
    }
    "stream the records in the database" in {
      db.users.transaction { transaction =>
        transaction.stream.map(_.age).toList.map(_.toSet).map { ages =>
          ages should be(Set(101, 42, 89, 102, 53, 13, 2, 22, 12, 81, 35, 63, 99, 23, 30, 4, 21, 33, 11, 72, 15, 62))
        }
      }
    }
    "verify the correct addresses" in {
      db.addresses.transaction { transaction =>
        transaction.stream.map(_.userId).toList.map { ids =>
          ids.sorted should be(addresses.map(_.userId).sorted)
        }
      }
    }
    "delete some records" in {
      db.users.transaction { transaction =>
        for
          d1 <- transaction.delete(linda._id)
          d2 <- transaction.delete(yuri._id)
        yield {
          d1 should be(true)
          d2 should be(true)
        }
      }
    }
    "verify the records were deleted" in {
      db.users.transaction { transaction =>
        transaction.count.map(_ should be(24))
      }
    }
    "modify a record" in {
      db.users.transaction { transaction =>
        transaction.modify(adam._id) {
          case Some(p) => Task.pure(Some(p.copy(name = "Allan")))
          case None => fail("Adam was not found!")
        }
      }.map {
        case Some(p) => p.name should be("Allan")
        case None => fail("Allan was not returned!")
      }
    }
    "verify the record has been renamed" in {
      db.users.transaction { transaction =>
        transaction(adam._id).map(_.name should be("Allan"))
      }
    }
    "insert a lot more names" in {
      db.users.transaction { transaction =>
        val p = (1 to CreateRecords).toList.map { index =>
          User(
            name = s"Unique Snowflake $index",
            age = if index > 100 then 0 else index,
          )
        }
        transaction.insert(p).succeed
      }
    }
    "verify the correct number of people exist in the database" in {
      db.users.transaction { transaction =>
        transaction.count.map(_ should be(CreateRecords + 24))
      }
    }
    "truncate the store again" in {
      db.users.transaction { transaction =>
        transaction.truncate.map(_ should be(CreateRecords + 24))
      }
    }
    "truncate the addresses store" in {
      db.addresses.transaction { transaction =>
        transaction.truncate.map(_ should be(3))
      }
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

    val users: Store[User, User.type] = store(User)
    val addresses: Store[Address, Address.type] = store(Address)

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class User(name: String, age: Int, _id: Id[User] = User.id()) extends Document[User]

  object User extends DocumentModel[User] with JsonConversion[User] {
    override implicit val rw: RW[User] = RW.gen

    val name: I[String] = field.index("name", _.name)
    val age: F[Int] = field("age", _.age)
  }

  case class Address(userId: Id[User], value: String, _id: Id[Address] = Address.id()) extends Document[Address]

  object Address extends DocumentModel[Address] with JsonConversion[Address] {
    override implicit val rw: RW[Address] = RW.gen

    val userId: I[Id[User]] = field.index("userId", _.userId)
    val value: F[String] = field("value", _.value)
  }

  private def deleteDirectoryIfExists(path: Path): Unit = {
    if Files.exists(path) then {
      Files.walk(path)
        .sorted(Comparator.reverseOrder())
        .forEach(Files.delete(_))
    }
  }
}
