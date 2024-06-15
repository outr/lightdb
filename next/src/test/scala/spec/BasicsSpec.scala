package spec

import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw.RW
import lightdb.document.{Document, DocumentModel}
import lightdb.store.{AtomicMapStore, StoreManager}
import lightdb.{Id, LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class BasicsSpec extends AsyncWordSpec with AsyncIOSpec with Matchers {
  "Basics" should {
    "initialize the database" in {
      fail()
    }
    "insert the first record" in {
      fail()
    }
    "retrieve the first record by id" in {
      fail()
    }
    "dispose the database" in {
      fail()
    }
  }

  object DB extends LightDB {
    val people = collection(Person)

    override def storeManager: StoreManager = AtomicMapStore
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] {
    implicit val rw: RW[Person] = RW.gen
  }
}
