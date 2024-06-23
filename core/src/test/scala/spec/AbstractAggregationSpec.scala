package spec

import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw._
import lightdb.document.{Document, DocumentModel}
import lightdb.index.{Indexed, IndexedCollection, Indexer}
import lightdb.query.SortDirection
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Id, LightDB, StoredValue}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Path, Paths}

abstract class AbstractAggregationSpec extends AsyncWordSpec with AsyncIOSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val adam = Person("Adam", 21)
  private val brenda = Person("Brenda", 11)
  private val charlie = Person("Charlie", 35)
  private val diana = Person("Diana", 15)
  private val evan = Person("Evan", 53)
  private val fiona = Person("Fiona", 23)
  private val greg = Person("Greg", 12)
  private val hanna = Person("Hanna", 62)
  private val ian = Person("Ian", 89)
  private val jenna = Person("Jenna", 4)
  private val kevin = Person("Kevin", 33)
  private val linda = Person("Linda", 72)
  private val mike = Person("Mike", 42)
  private val nancy = Person("Nancy", 22)
  private val oscar = Person("Oscar", 21)
  private val penny = Person("Penny", 2)
  private val quintin = Person("Quintin", 99)
  private val ruth = Person("Ruth", 102)
  private val sam = Person("Sam", 81)
  private val tori = Person("Tori", 30)

  private val names = List(
    adam, brenda, charlie, diana, evan, fiona, greg, hanna, ian, jenna, kevin, linda, mike, nancy, oscar, penny,
    quintin, ruth, sam, tori
  )

  "Aggregation" should {
    "initialize the database" in {
      DB.init().map(_ should be(true))
    }
    "insert people" in {
      DB.people.transaction { implicit transaction =>
        DB.people.set(names).map { count =>
          count should be(20)
        }
      }
    }
    "get a basic aggregation" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query
          .filter(p => p.age <=> 5 -> 16)
          .aggregate(p => List(p.age.max, p.age.min, p.age.avg, p.age.sum, p.age.count))
          .stream
          .compile
          .toList
          .map { list =>
            list.map(m => m(p => p.age.max)) should be(List(15))
            list.map(m => m(p => p.age.min)) should be(List(11))
            list.map(m => m(p => p.age.avg)) should be(List(12.666666666666666))
            list.map(m => m(p => p.age.sum)) should be(List(38))
            list.map(m => m(p => p.age.count)) should be(List(3))
          }
      }
    }
    "aggregate with grouping and filtering" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query
          .aggregate(p => List(p._id.concat, p.name.concat, p.age.group, p.age.count))
          .sort(Person.age.count, SortDirection.Descending)
          .filter(Person.age.count > 1)
          .toList
          .map { list =>
            list.map(_(_.name.concat)).map(_.toSet) should be(List(Set("Oscar", "Adam")))
            list.map(_(_._id.concat)).map(_.toSet) should be(List(Set(oscar._id, adam._id)))
            list.map(_(_.age.group)) should be(List(21))
            list.map(_(_.age.count)) should be(List(2))
          }
      }
    }
    "aggregate with age concatenation" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query
          .aggregate(p => List(p.age.concat))
          .toList
          .map { list =>
            list.map(_(_.age.concat).toSet) should be(List(
              Set(2, 4, 11, 12, 15, 21, 21, 22, 23, 30, 33, 35, 42, 53, 62, 72, 81, 89, 99, 102)
            ))
          }
      }
    }
    "dispose" in {
      DB.dispose()
    }
  }

  protected def storeManager: StoreManager
  protected def indexer(model: Person.type): Indexer[Person, Person.type]

  object DB extends LightDB {
    override lazy val directory: Path = Path.of(s"db/$specName")
    override protected def truncateOnInit: Boolean = true

    val people: IndexedCollection[Person, Person.type] = collection("people", Person, indexer(Person))

    override def storeManager: StoreManager = spec.storeManager

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Id()) extends Document[Person]

  object Person extends DocumentModel[Person] with Indexed[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: I[String] = index.one("name", _.name)
    val age: I[Int] = index.one("age", _.age)
  }
}