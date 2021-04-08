package spec

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import fabric.rw.{ReaderWriter, ccRW}
import lightdb.collection.Collection
import lightdb.{Document, Id, LightDB, ObjectMapping}
import lightdb.data.{DataManager, JsonDataManager}
import lightdb.field.Field
import lightdb.index._
import lightdb.store.HaloStore
import testy.{AsyncSupport, Spec}

import scala.concurrent.Future

class SimpleSpec extends Spec {
  private object IOAsyncSupport extends AsyncSupport[IO[Any]] {
    override def apply(async: IO[Any]): Future[Unit] = async.unsafeToFuture()(IORuntime.global).asInstanceOf[Future[Unit]]
  }
  implicit def asyncSupport[T]: AsyncSupport[IO[T]] = IOAsyncSupport.asInstanceOf[AsyncSupport[IO[T]]]

  private val id1 = Id[Person]("john")
  private val id2 = Id[Person]("jane")

  private val p1 = Person("John Doe", 21, id1)
  private val p2 = Person("Jane Doe", 19, id2)

  "Simple database" should {
    "store John Doe" async {
      db.people.put(p1).map { p =>
        p._id should be(id1)
      }
    }
    "verify John Doe exists" async {
      db.people.get(id1).map { o =>
        o should be(Some(p1))
      }
    }
    "storage Jane Doe" async {
      db.people.put(p2).map { p =>
        p._id should be(id2)
      }
    }
    "verify Jane Doe exists" async {
      db.people.get(id2).map { o =>
        o should be(Some(p2))
      }
    }
    "verify exactly two objects in data" async {
      db.store.count().map { size =>
        size should be(2)
      }
    }
    "flush data" async {
      db.people.commit()
    }
    "verify exactly two objects in index" async {
      db.people.indexer.count().map { size =>
        size should be(2)
      }
    }
    "delete John" async {
      db.people.delete(id1)
    }
    "verify exactly one object in data" async {
      db.store.count().map { size =>
        size should be(1)
      }
    }
    "commit data" async {
      db.people.commit()
    }
    "verify exactly one object in index" async {
      db.people.indexer.count().map { size =>
        size should be(1)
      }
    }
    "list all documents" async {
      db.people.indexer.search().map { results =>
        results.total should be(1)
        val doc = results.documents.head
        doc.id should be(id2)
        doc(Person.name) should be("Jane Doe")
        doc(Person.age) should be(19)
      }
    }
    // TODO: search for an item
    "replace Jane Doe" async {
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
    "commit data" async {
      db.people.commit()
    }
    "list all documents" async {
      db.people.indexer.search().map { results =>
        results.total should be(1)
        val doc = results.documents.head
        doc.id should be(id2)
        doc(Person.name) should be("Jan Doe")
        doc(Person.age) should be(20)
      }
    }
    // TODO: support multiple item types (make sure queries don't return different types)
    // TODO: test batch operations: insert, replace, and delete
    "dispose" async {
      db.dispose()
    }
  }

  object db extends LightDB(None, HaloStore()) with LuceneIndexerSupport {
    val people: Collection[Person] = collection("people", Person)
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Id()) extends Document[Person]

  object Person extends ObjectMapping[Person] {
    implicit val rw: ReaderWriter[Person] = ccRW

    lazy val dataManager: DataManager[Person] = new JsonDataManager[Person]

    lazy val name: Field[Person, String] = field[String]("name", _.name).indexed
    lazy val age: Field[Person, Int] = field[Int]("age", _.age).indexed

    override lazy val fields: List[Field[Person, _]] = List(name, age)
  }
}