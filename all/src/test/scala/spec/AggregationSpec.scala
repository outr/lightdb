package spec

import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw._
import lightdb.aggregate.AggregateType
import lightdb.{Document, Id, IndexedLinks, LightDB, StoredValue}
import lightdb.halo.HaloDBSupport
import lightdb.model.Collection
import lightdb.query.SortDirection
import lightdb.sqlite.SQLiteSupport
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Path, Paths}

class AggregationSpec extends AsyncWordSpec with AsyncIOSpec with Matchers {
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
      DB.init(truncate = true)
    }
    "insert people" in {
      Person.setAll(names).map { count =>
        count should be(20)
      }
    }
    "commit the changes" in {
      Person.commit()
    }
    "get a basic aggregation" in {
      val max = Person.age.max()
      val min = Person.age.min()
      val avg = Person.age.avg()
      val sum = Person.age.sum()
      val count = Person.age.count()

      Person.withSearchContext { implicit context =>
        Person.query
          .filter(Person.age <=> (5, 16))
          .aggregate(max, min, avg, sum, count)
          .stream
          .compile
          .toList
          .map { list =>
            list.map(m => m(max)) should be(List(15))
            list.map(m => m(min)) should be(List(11))
            list.map(m => m(avg)) should be(List(12.666666666666666))
            list.map(m => m(sum)) should be(List(38))
            list.map(m => m(count)) should be(List(3))
          }
      }
    }
    "aggregate with grouping and filtering" in {
      val ids = Person._id.concat()
      val names = Person.name.concat()
      val age = Person.age.group()
      val count = Person.age.count()
      Person.query
        .aggregate(ids, names, age, count)
        .sort(count, SortDirection.Descending)
        .filter(count > 1)
        .toList
        .map { list =>
        list.map(_(names)).map(_.toSet) should be(List(Set("Oscar", "Adam")))
        list.map(_(ids)).map(_.toSet) should be(List(Set(oscar._id, adam._id)))
        list.map(_(age)) should be(List(21))
        list.map(_(count)) should be(List(2))
      }
    }
    "aggregate with age concatenation" in {
      val ages = Person.age.concat()
      Person.query
        .aggregate(ages)
        .toList
        .map { list =>
          list.map(_(ages).toSet) should be(List(
            Set(2, 4, 11, 12, 15, 21, 21, 22, 23, 30, 33, 35, 42, 53, 62, 72, 81, 89, 99, 102)
          ))
        }
    }
    "dispose" in {
      DB.dispose()
    }
  }

  object DB extends LightDB with HaloDBSupport {
    override lazy val directory: Path = Paths.get("db/aggregation")

    val startTime: StoredValue[Long] = stored[Long]("startTime", -1L)

    override lazy val userCollections: List[Collection[_]] = List(
      Person
    )

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Id()) extends Document[Person]

  object Person extends Collection[Person]("people", DB) with SQLiteSupport[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: I[String] = index.one("name", _.name)
    val age: I[Int] = index.one("age", _.age)
  }
}
