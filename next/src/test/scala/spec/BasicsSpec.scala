package spec

import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw.RW
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.store.{AtomicMapStore, StoreManager}
import lightdb.{Id, LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class BasicsSpec extends AsyncWordSpec with AsyncIOSpec with Matchers {
  private val amy = Person("Amy", 21)

  "Basics" should {
    "initialize the database" in {
      DB.init
    }
    "insert the first record" in {
      DB.people.transaction { implicit transaction =>
        DB.people.set(amy).map(o => o should not be None)
      }
    }
    "retrieve the first record by id" in {
      DB.people.transaction { implicit transaction =>
        DB.people(amy._id).map { p =>
          p should be(amy)
        }
      }
    }
    "dispose the database" in {
      DB.dispose()
    }
  }

  object DB extends LightDB {
    val people: Collection[Person] = collection("people", Person)

    override def storeManager: StoreManager = AtomicMapStore
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] {
    implicit val rw: RW[Person] = RW.gen
  }
}
