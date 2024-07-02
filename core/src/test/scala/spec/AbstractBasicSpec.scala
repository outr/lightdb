package spec

import fabric.rw._
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Field, Id, LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Path

abstract class AbstractBasicSpec extends AnyWordSpec with Matchers { spec =>
  private val adam = Person("Adam", 21, Person.id("adam"))
  private val brenda = Person("Brenda", 11, Person.id("brenda"))
  private val charlie = Person("Charlie", 35, Person.id("charlie"))
  private val diana = Person("Diana", 15, Person.id("diana"))
  private val evan = Person("Evan", 53, Person.id("evan"))
  private val fiona = Person("Fiona", 23, Person.id("fiona"))
  private val greg = Person("Greg", 12, Person.id("greg"))
  private val hanna = Person("Hanna", 62, Person.id("hanna"))
  private val ian = Person("Ian", 89, Person.id("ian"))
  private val jenna = Person("Jenna", 4, Person.id("jenna"))
  private val kevin = Person("Kevin", 33, Person.id("kevin"))
  private val linda = Person("Linda", 72, Person.id("linda"))
  private val mike = Person("Mike", 42, Person.id("mike"))
  private val nancy = Person("Nancy", 22, Person.id("nancy"))
  private val oscar = Person("Oscar", 21, Person.id("oscar"))
  private val penny = Person("Penny", 2, Person.id("penny"))
  private val quintin = Person("Quintin", 99, Person.id("quintin"))
  private val ruth = Person("Ruth", 102, Person.id("ruth"))
  private val sam = Person("Sam", 81, Person.id("sam"))
  private val tori = Person("Tori", 30, Person.id("tori"))
  private val uba = Person("Uba", 21, Person.id("uba"))
  private val veronica = Person("Veronica", 13, Person.id("veronica"))
  private val wyatt = Person("Wyatt", 30, Person.id("wyatt"))
  private val xena = Person("Xena", 63, Person.id("xena"))
  private val yuri = Person("Yuri", 30, Person.id("yuri"))
  private val zoey = Person("Zoey", 101, Person.id("zoey"))

  private val names = List(
    adam, brenda, charlie, diana, evan, fiona, greg, hanna, ian, jenna, kevin, linda, mike, nancy, oscar, penny,
    quintin, ruth, sam, tori, uba, veronica, wyatt, xena, yuri, zoey
  )

  private lazy val specName: String = getClass.getSimpleName

  specName should {
    "initialize the database" in {
      DB.init() should be(true)
    }
    "verify the database is empty" in {
      DB.people.transaction { implicit transaction =>
        DB.people.count should be(0)
      }
    }
    "insert the records" in {
      DB.people.transaction { implicit transaction =>
        DB.people.set(names) should not be None
      }
    }
    "retrieve the first record by id" in {
      DB.people.transaction { implicit transaction =>
        DB.people(_._id -> adam._id) should be(adam)
      }
    }
    "count the records in the database" in {
      DB.people.transaction { implicit transaction =>
        DB.people.count should be(26)
      }
    }
    "stream the ids in the database" in {
      DB.people.transaction { implicit transaction =>
        val ids = DB.people.query.search.value(Person._id).iterator.toList.toSet
        ids should be(names.map(_._id).toSet)
      }
    }
    "stream the records in the database" in {
      DB.people.transaction { implicit transaction =>
        val ages = DB.people.iterator.map(_.age).toSet
        ages should be(Set(101, 42, 89, 102, 53, 13, 2, 22, 12, 81, 35, 63, 99, 23, 30, 4, 21, 33, 11, 72, 15, 62))
      }
    }
    "delete some records" in {
      DB.people.transaction { implicit transaction =>
        DB.people.delete(_._id -> linda._id) should be(true)
        DB.people.delete(_._id -> yuri._id) should be(true)
      }
    }
    "verify the records were deleted" in {
      DB.people.transaction { implicit transaction =>
        DB.people.count should be(24)
      }
    }
    "modify a record" in {
      DB.people.transaction { implicit transaction =>
        DB.people.modify(adam._id) {
          case Some(p) => Some(p.copy(name = "Allan"))
          case None => fail("Adam was not found!")
        }
      } match {
        case Some(p) => p.name should be("Allan")
        case None => fail("Allan was not returned!")
      }
    }
    "verify the record has been renamed" in {
      DB.people.transaction { implicit transaction =>
        DB.people(_._id -> adam._id).name should be("Allan")
      }
    }
    "truncate the collection" in {
      DB.people.transaction { implicit transaction =>
        DB.people.truncate() should be(24)
      }
    }
    "verify the collection is empty" in {
      DB.people.transaction { implicit transaction =>
        DB.people.count should be(0)
      }
    }
    "dispose the database" in {
      DB.dispose()
    }
  }

  def storeManager: StoreManager

  object DB extends LightDB {
    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val people: Collection[Person, Person.type] = collection("people", Person)

    override def storeManager: StoreManager = spec.storeManager

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    implicit val rw: RW[Person] = RW.gen

    val name: Field[Person, String] = field("name", _.name)
    val age: Field[Person, Int] = field("age", _.age)
  }
}
