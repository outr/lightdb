package spec

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw._
import lightdb._
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.Paths

class SimpleSpec extends AsyncWordSpec with AsyncIOSpec with Matchers {
  private val id1 = Id[Person]("john")
  private val id2 = Id[Person]("jane")

  private val p1 = Person("John Doe", 21, id1)
  private val p2 = Person("Jane Doe", 19, id2)

  "Simple database" should {
    "initialize the database" in {
      DB.init(truncate = true)
    }
    "store John Doe" in {
      Person.set(p1).map { p =>
        p._id should be(id1)
      }
    }
    "verify John Doe exists" in {
      Person.get(id1).map { o =>
        o should be(Some(p1))
      }
    }
    "storage Jane Doe" in {
      Person.set(p2).map { p =>
        p._id should be(id2)
      }
    }
    "verify Jane Doe exists" in {
      Person.get(id2).map { o =>
        o should be(Some(p2))
      }
    }
    "verify exactly two objects in data" in {
      Person.size.map { size =>
        size should be(2)
      }
    }
    "flush data" in {
      Person.commit()
    }
//    "verify exactly two objects in index" in {
//      db.people.indexer.count().map { size =>
//        size should be(2)
//      }
//    }
    "verify exactly two objects in the store" in {
      Person.idStream.compile.toList.map { ids =>
        ids.toSet should be(Set(id1, id2))
      }
    }
    "search by name for positive result" in {
      Person.name.query("Jane Doe").compile.toList.map { people =>
        people.length should be(1)
        val p = people.head
        p._id should be(id2)
        p.name should be("Jane Doe")
        p.age should be(19)
      }
    }
    "search by age for positive result" in {
      Person.age.query(19).compile.toList.map { people =>
        people.length should be(1)
        val p = people.head
        p._id should be(id2)
        p.name should be("Jane Doe")
        p.age should be(19)
      }
    }
    "search by id for John" in {
      Person(id1).map { person =>
        person._id should be(id1)
        person.name should be("John Doe")
        person.age should be(21)
      }
    }
    "delete John" in {
      Person.delete(id1).map { deleted =>
        deleted should not be empty
      }
    }
    "verify exactly one object in data" in {
      Person.size.map { size =>
        size should be(1)
      }
    }
    "commit data" in {
      Person.commit()
    }
//    "verify exactly one object in index" in {
//      db.people.indexer.count().map { size =>
//        size should be(1)
//      }
//    }
    "list all documents" in {
      Person.stream.compile.toList.map { people =>
        people.length should be(1)
        val p = people.head
        p._id should be(id2)
        p.name should be("Jane Doe")
        p.age should be(19)
      }
    }
    // TODO: search for an item by name and by age range
    "replace Jane Doe" in {
      Person.set(Person("Jane Doe", 20, id2)).map { p =>
        p._id should be(id2)
      }
    }
    "verify Jan Doe" in {
      Person(id2).map { p =>
        p._id should be(id2)
        p.name should be("Jan Doe")
        p.age should be(20)
      }
    }
    "commit new data" in {
      Person.commit()
    }
    "list new documents" in {
      Person.stream.compile.toList.map { results =>
        results.length should be(1)
        val doc = results.head
        doc._id should be(id2)
        doc.name should be("Jan Doe")
        doc.age should be(20)
      }
    }
    "verify start time has been set" in {
      DB.startTime.get().map { startTime =>
        startTime should be > 0L
      }
    }
    // TODO: support multiple item types (make sure queries don't return different types)
    // TODO: test batch operations: insert, replace, and delete
    "dispose" in {
      DB.dispose()
    }
  }

  object DB extends LightDB(directory = Paths.get("testdb")) {
//    override protected def autoCommit: Boolean = true

    val startTime: StoredValue[Long] = stored[Long]("startTime", -1L)

//    val people: Collection[Person] = collection("people", Person)

    override lazy val collections: List[Collection[_]] = List(
      Person
    )

    override def upgrades: List[DatabaseUpgrade] = List(InitialSetupUpgrade)
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Id()) extends Document[Person]

  object Person extends Collection[Person]("people", DB) {
    override implicit val rw: RW[Person] = RW.gen

    val name: IndexedLinks[String, Person] = indexedLinks[String]("names", identity, _.name)
    val age: IndexedLinks[Int, Person] = indexedLinks[Int]("age", _.toString, _.age)
  }

  object InitialSetupUpgrade extends DatabaseUpgrade {
    override def applyToNew: Boolean = true
    override def blockStartup: Boolean = true
    override def alwaysRun: Boolean = false

    override def upgrade(ldb: LightDB): IO[Unit] = DB.startTime.set(System.currentTimeMillis()).map(_ => ())
  }
}