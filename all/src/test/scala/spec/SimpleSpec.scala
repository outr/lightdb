package spec

import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw._
import lighdb.storage.mapdb.SharedMapDBSupport
import lightdb.collection.Collection
import lightdb.index.lucene._
import lightdb.query._
import lightdb._
import lightdb.store.halo.MultiHaloSupport
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
      db.init(truncate = true)
    }
    "store John Doe" in {
      db.people.put(p1).map { p =>
        p._id should be(id1)
      }
    }
    "verify John Doe exists" in {
      db.people.get(id1).map { o =>
        o should be(Some(p1))
      }
    }
    "storage Jane Doe" in {
      db.people.put(p2).map { p =>
        p._id should be(id2)
      }
    }
    "verify Jane Doe exists" in {
      db.people.get(id2).map { o =>
        o should be(Some(p2))
      }
    }
    "verify exactly two objects in data" in {
      db.people.store.count().map { size =>
        size should be(2)
      }
    }
    "flush data" in {
      db.people.commit()
    }
    "verify exactly two objects in index" in {
      db.people.indexer.count().map { size =>
        size should be(2)
      }
    }
    "verify exactly two objects in the store" in {
      db.people.store.all[Person]()
        .compile
        .toList
        .map(_.map(_.id))
        .map { ids =>
          ids.toSet should be(Set(id1, id2))
        }
    }
    "search by name for positive result" in {
      db.people.query.filter(Person.name is "Jane Doe").search().compile.toList.map { results =>
        results.length should be(1)
        val doc = results.head
        doc.id should be(id2)
        doc(Person.name) should be("Jane Doe")
        doc(Person.age) should be(19)
      }
    }
    "search by age for positive result" in {
      db.people.query.filter(Person.age is 19).search().compile.toList.map { results =>
        results.length should be(1)
        val doc = results.head
        doc.id should be(id2)
        doc(Person.name) should be("Jane Doe")
        doc(Person.age) should be(19)
      }
    }
    "search by id for John" in {
      db.people.query.filter(Person._id is id1).search().compile.toList.map { results =>
        results.length should be(1)
        val doc = results.head
        doc.id should be(id1)
        doc(Person.name) should be("John Doe")
        doc(Person.age) should be(21)
      }
    }
    "delete John" in {
      db.people.delete(id1)
    }
    "verify exactly one object in data" in {
      db.people.store.count().map { size =>
        size should be(1)
      }
    }
    "commit data" in {
      db.people.commit()
    }
    "verify exactly one object in index" in {
      db.people.indexer.count().map { size =>
        size should be(1)
      }
    }
    "list all documents" in {
      db.people.query.search().compile.toList.flatMap { results =>
        results.length should be(1)
        val doc = results.head
        doc.id should be(id2)
        doc(Person.name) should be("Jane Doe")
        doc(Person.age) should be(19)
        doc.get().map { person =>
          person._id should be(id2)
          person.name should be("Jane Doe")
          person.age should be(19)
        }
      }
    }
    // TODO: search for an item by name and by age range
    "replace Jane Doe" in {
      db.people.put(Person("Jan Doe", 20, id2)).map { p =>
        p._id should be(id2)
      }
    }
    "verify Jan Doe" in {
      db.people(id2).map { p =>
        p._id should be(id2)
        p.name should be("Jan Doe")
        p.age should be(20)
      }
    }
    "commit new data" in {
      db.people.commit()
    }
    "list new documents" in {
      db.people.query.search().compile.toList.map { results =>
        results.length should be(1)
        val doc = results.head
        doc.id should be(id2)
        doc(Person.name) should be("Jan Doe")
        doc(Person.age) should be(20)
      }
    }
    // TODO: support multiple item types (make sure queries don't return different types)
    // TODO: test batch operations: insert, replace, and delete
    "dispose" in {
      db.dispose()
    }
  }

  object db extends LightDB(directory = Some(Paths.get("testdb"))) with LuceneIndexerSupport with MultiHaloSupport {
    override protected def autoCommit: Boolean = true

    val people: Collection[Person] = collection("people", Person)
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Id()) extends Document[Person]

  object Person extends JsonMapping[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: FD[String] = field("name", _.name)
    val age: FD[Int] = field("age", _.age)
  }
}