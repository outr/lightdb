package spec

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw.RW
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.store.StoreManager
import lightdb.{Id, LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

abstract class AbstractStoreSpec extends AsyncWordSpec with AsyncIOSpec with Matchers { spec =>
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
  private val uba = Person("Uba", 21)
  private val veronica = Person("Veronica", 13)
  private val wyatt = Person("Wyatt", 30)
  private val xena = Person("Xena", 63)
  private val yuri = Person("Yuri", 30)
  private val zoey = Person("Zoey", 101)

  private val names = List(
    adam, brenda, charlie, diana, evan, fiona, greg, hanna, ian, jenna, kevin, linda, mike, nancy, oscar, penny,
    quintin, ruth, sam, tori, uba, veronica, wyatt, xena, yuri, zoey
  )

  private lazy val specName: String = getClass.getSimpleName

  specName should {
    "initialize the database" in {
      DB.init().map(b => b should be(true))
    }
    "insert the records" in {
      DB.people.transaction { implicit transaction =>
        DB.people.set(names).map(o => o should not be None)
      }
    }
    "retrieve the first record by id" in {
      DB.people.transaction { implicit transaction =>
        DB.people(adam._id).map { p =>
          p should be(adam)
        }
      }
    }
    "count the records in the database" in {
      DB.people.transaction { implicit transaction =>
        DB.people.count.map { count =>
          count should be(26)
        }
      }
    }
    "stream the ids in the database" in {
      DB.people.transaction { implicit transaction =>
        DB.people.idStream.compile.toList.map(_.toSet).map { ids =>
          ids should be(names.map(_._id).toSet)
        }
      }
    }
    "stream the records in the database" in {
      DB.people.transaction { implicit transaction =>
        DB.people.stream.map(_.age).compile.toList.map(_.toSet).map { ages =>
          ages should be(Set(101, 42, 89, 102, 53, 13, 2, 22, 12, 81, 35, 63, 99, 23, 30, 4, 21, 33, 11, 72, 15, 62))
        }
      }
    }
    "delete some records" in {
      DB.people.transaction { implicit transaction =>
        DB.people.delete(List(linda, yuri)).map { deleted =>
          deleted should be(2)
        }
      }
    }
    "verify the records were deleted" in {
      DB.people.transaction { implicit transaction =>
        DB.people.count.map { count =>
          count should be(24)
        }
      }
    }
    "modify a record" in {
      DB.people.transaction { implicit transaction =>
        DB.people.modify(adam._id) {
          case Some(p) => IO.pure(Some(p.copy(name = "Allan")))
          case None => fail("Adam was not found!")
        }
      }.map {
        case Some(p) => p.name should be("Allan")
        case None => fail("Allan was not returned!")
      }
    }
    "verify the record has been renamed" in {
      DB.people.transaction { implicit transaction =>
        DB.people(adam._id).map { p =>
          p.name should be("Allan")
        }
      }
    }
    "truncate the collection" in {
      DB.people.transaction { implicit transaction =>
        DB.people.truncate().map { removed =>
          removed should be(24)
        }
      }
    }
    "verify the collection is empty" in {
      DB.people.transaction { implicit transaction =>
        DB.people.count.map { count =>
          count should be(0)
        }
      }
    }
    "dispose the database" in {
      DB.dispose()
    }
  }

  protected def storeManager: StoreManager

  object DB extends LightDB {
    val people: Collection[Person] = collection("people", Person)

    override def storeManager: StoreManager = spec.storeManager
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] {
    implicit val rw: RW[Person] = RW.gen
  }
}