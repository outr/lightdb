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
import org.apache.lucene.store.FSDirectory
import testy.{AsyncSupport, Spec}

import java.nio.file.Paths
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
    "commit to index" async {
      db.people.flush()
    }
    "verify exactly two objects in index" async {
      db.indexer.count().map { size =>
        size should be(2)
      }
    }
    "dispose" async {
      db.dispose()
    }
  }

  object db extends LightDB(HaloStore(), LuceneIndexer()) {
    val people: Collection[Person] = collection(Person)
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
