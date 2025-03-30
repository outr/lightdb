package spec

import fabric.rw._
import lightdb.{Id, LightDB, store}
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field
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
        override val storeManager: MapStore.type = MapStore
        override def upgrades: List[DatabaseUpgrade] = Nil

        lazy val people: MapStore[Person, Person.type] = store[Person, Person.type](Person)
      }

      TestDB.init.sync()

      TestDB.people shouldBe a[MapStore[_, _]]

      TestDB.people.t.insert(Person("Test 1", 1)).sync()

      val mapStore: MapStore[Person, Person.type] = TestDB.people
      mapStore.map.size should be(1)

      TestDB.dispose.sync()
    }
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: F[String] = field("name", _.name)
    val age: F[Int] = field("age", _.age)
  }
}
