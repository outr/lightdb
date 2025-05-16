package spec

import fabric.rw._
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.id.Id
import lightdb.store.hashmap.HashMapStore
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
        override type SM = HashMapStore.type

        override def directory: Option[Path] = None
        override val storeManager: HashMapStore.type = HashMapStore
        override def upgrades: List[DatabaseUpgrade] = Nil

        lazy val people: HashMapStore[Person, Person.type] = store[Person, Person.type](Person)
      }

      TestDB.init.sync()

      TestDB.people shouldBe a[HashMapStore[_, _]]

      TestDB.people.t.insert(Person("Test 1", 1)).sync()

      val mapStore: HashMapStore[Person, Person.type] = TestDB.people
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
