package spec

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw._
import lightdb.async.{AsyncCollection, AsyncDatabaseUpgrade, AsyncLightDB, AsyncStoredValue}
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.feature.DBFeatureKey
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Id, LightDB, Sort, StoredValue}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}
import perfolation.double2Implicits

import java.nio.file.Path

abstract class AbstractAsyncSpec extends AsyncWordSpec with AsyncIOSpec with Matchers { spec =>
  protected def aggregationSupported: Boolean = true

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

  private var features = Map.empty[DBFeatureKey[Any], Any]
  protected def addFeature[T](key: DBFeatureKey[T], value: T): Unit =
    features += key.asInstanceOf[DBFeatureKey[Any]] -> value

  protected lazy val specName: String = getClass.getSimpleName

  protected var db: DB = new DB

  specName should {
    "initialize the database" in {
      db.init().map(b => b should be(true))
    }
    "verify the database is empty" in {
      db.people.transaction { implicit transaction =>
        db.people.count.map(c => c should be(0))
      }
    }
    "insert the records" in {
      db.people.transaction { implicit transaction =>
        db.people.insert(names).map(_ should not be None)
      }
    }
    "retrieve the first record by _id -> id" in {
      db.people.transaction { implicit transaction =>
        db.people(_._id -> adam._id).map(_ should be(adam))
      }
    }
    "retrieve the first record by id" in {
      db.people.transaction { implicit transaction =>
        db.people(adam._id).map(_ should be(adam))
      }
    }
    "count the records in the database" in {
      db.people.transaction { implicit transaction =>
        db.people.count.map(_ should be(26))
      }
    }
    "stream the ids in the database" in {
      db.people.transaction { implicit transaction =>
        db.people.query.search.id.flatMap(_.stream.compile.toList).map(_.toSet).map { ids =>
          ids should be(names.map(_._id).toSet)
        }
      }
    }
    "stream the records in the database" in {
      db.people.transaction { implicit transaction =>
        db.people.stream.compile.toList.map(_.map(_.age).toSet).map { ages =>
          ages should be(Set(101, 42, 89, 102, 53, 13, 2, 22, 12, 81, 35, 63, 99, 23, 30, 4, 21, 33, 11, 72, 15, 62))
        }
      }
    }
    "query with aggregate functions" in {
      if (aggregationSupported) {
        db.people.transaction { implicit transaction =>
          db.people.query
            .aggregate(p => List(
              p.age.min,
              p.age.max,
              p.age.avg,
              p.age.sum
            ))
            .toList
            .map { list =>
              list.map(m => m(_.age.min)).toSet should be(Set(2))
              list.map(m => m(_.age.max)).toSet should be(Set(102))
              list.map(m => m(_.age.avg).f(f = 6)).toSet should be(Set("41.807692"))
              list.map(m => m(_.age.sum)).toSet should be(Set(1087))
            }
        }
      } else {
        succeed
      }
    }
    "search by age range" in {
      db.people.transaction { implicit transaction =>
        db.people.query.filter(_.age BETWEEN 19 -> 22).search.id.flatMap(_.stream.compile.toList).map { ids =>
          ids.toSet should be(Set(adam._id, nancy._id, oscar._id, uba._id))
        }
      }
    }
    "sort by age" in {
      db.people.transaction { implicit transaction =>
        db.people.query.sort(Sort.ByField(Person.age).descending).search.docs.flatMap(_.stream.compile.toList).map { people =>
          people.map(_.name).take(3) should be(List("Ruth", "Zoey", "Quintin"))
        }
      }
    }
    "group by age" in {
      db.people.transaction { implicit transaction =>
        db.people.query.grouped(_.age).compile.toList.map { list =>
          list.map(_._1) should be(List(2, 4, 11, 12, 13, 15, 21, 22, 23, 30, 33, 35, 42, 53, 62, 63, 72, 81, 89, 99, 101, 102))
          list.map(_._2.map(_.name).toSet) should be(List(
            Set("Penny"), Set("Jenna"), Set("Brenda"), Set("Greg"), Set("Veronica"), Set("Diana"),
            Set("Adam", "Uba", "Oscar"), Set("Nancy"), Set("Fiona"), Set("Tori", "Yuri", "Wyatt"), Set("Kevin"),
            Set("Charlie"), Set("Mike"), Set("Evan"), Set("Hanna"), Set("Xena"), Set("Linda"), Set("Sam"), Set("Ian"),
            Set("Quintin"), Set("Zoey"), Set("Ruth")
          ))
        }
      }
    }
    "delete some records" in {
      db.people.transaction { implicit transaction =>
        for {
          b1 <- db.people.delete(_._id -> linda._id)
          b2 <- db.people.delete(_._id -> yuri._id)
        } yield (b1, b2) should be((true, true))
      }
    }
    "verify the records were deleted" in {
      db.people.transaction { implicit transaction =>
        db.people.count.map(_ should be(24))
      }
    }
    "modify a record" in {
      db.people.transaction { implicit transaction =>
        db.people.modify(adam._id) {
          case Some(p) => IO(Some(p.copy(name = "Allan")))
          case None => fail("Adam was not found!")
        }
      }.map {
        case Some(p) => p.name should be("Allan")
        case None => fail("Allan was not returned!")
      }
    }
    "verify the record has been renamed" in {
      db.people.transaction { implicit transaction =>
        db.people(_._id -> adam._id).map(_.name should be("Allan"))
      }
    }
    "verify start time has been set" in {
      db.startTime.get.map(_ should be > 0L)
    }
    "dispose the database before creating a new instance" in {
      db.dispose().map(_ should be(true))
    }
    "prepare a new instance" in {
      db = new DB
      db.init().map(_ should be(true))
    }
    "query the database to verify records were persisted properly" in {
      db.people.transaction { implicit transaction =>
        db.people.stream.compile.toList.map(_.map(_.name).toSet).map(_ should be(Set(
          "Tori", "Ruth", "Nancy", "Jenna", "Hanna", "Wyatt", "Diana", "Ian", "Quintin", "Uba", "Oscar", "Kevin",
          "Penny", "Charlie", "Evan", "Sam", "Mike", "Brenda", "Zoey", "Allan", "Xena", "Fiona", "Greg", "Veronica"
        )))
      }
    }
    "truncate the collection" in {
      db.people.transaction { implicit transaction =>
        db.people.truncate().map(_ should be(24))
      }
    }
    "verify the collection is empty" in {
      db.people.transaction { implicit transaction =>
        db.people.count.map(_ should be(0))
      }
    }
    "dispose the database" in {
      db.dispose().map(_ should be(true))
    }
  }

  def storeManager: StoreManager

  class DB extends AsyncLightDB {
    spec.features.foreach {
      case (key, value) =>
        put(key, value)
    }

    lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val startTime: AsyncStoredValue[Long] = stored[Long]("startTime", -1L)

    val people: AsyncCollection[Person, Person.type] = collection(Person)

    override def storeManager: StoreManager = spec.storeManager

    override def upgrades: List[AsyncDatabaseUpgrade] = List(InitialSetupUpgrade)
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    implicit val rw: RW[Person] = RW.gen

    val name: F[String] = field("name", _.name)
    val age: F[Int] = field.index("age", _.age)
  }

  object InitialSetupUpgrade extends AsyncDatabaseUpgrade {
    override def applyToNew: Boolean = true

    override def blockStartup: Boolean = true

    override def alwaysRun: Boolean = false

    override def upgrade(ldb: AsyncLightDB): IO[Unit] = {
      db.startTime.set(System.currentTimeMillis()).void
    }
  }
}
