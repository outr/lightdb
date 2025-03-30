package spec

import fabric.rw._
import lightdb.{Id, LightDB, store}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.store.{MapStore, Store, StoreManager}
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Path

@EmbeddedTest
class TypeSafetySpec extends AnyWordSpec with Matchers {
  "LightDB.store" should {
    "return the specific store type" in {
      // Create a simple in-memory database
      object TestDB extends LightDB {
        override type SM = MapStore.type

        override def directory: Option[Path] = None
        override def storeManager: MapStore.type = MapStore
        override def upgrades: List[DatabaseUpgrade] = Nil

        // This should return a MapStore[Person, Person.type], not a generic Store[Person, Person.type]
        lazy val people: MapStore[Person, Person.type] = store[Person, Person.type](Person)
      }

      // Initialize the database
      TestDB.init.sync()

      // Verify that the store is of the correct type
      TestDB.people shouldBe a[MapStore[_, _]]

      // This should compile because people is a MapStore, not a generic Store
      val mapStore: MapStore[Person, Person.type] = TestDB.people

      // Clean up
      TestDB.dispose.sync()
    }
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name = field("name", _.name)
    val age = field("age", _.age)
  }
}
