package spec

import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw.RW
import lightdb.{Id, LightDB}
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.index.{Indexed, IndexedCollection, Indexer}
import lightdb.spatial.GeoPoint
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.Path

abstract class AbstractIndexSpec extends AsyncWordSpec with AsyncIOSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val id1 = Id[Person]("john")
  private val id2 = Id[Person]("jane")
  private val id3 = Id[Person]("bob")

  private val newYorkCity = GeoPoint(40.7142, -74.0119)
  private val chicago = GeoPoint(41.8119, -87.6873)
  private val noble = GeoPoint(35.1417, -97.3409)
  private val oklahomaCity = GeoPoint(35.5514, -97.4075)
  private val yonkers = GeoPoint(40.9461, -73.8669)

  private val p1 = Person(
    name = "John Doe",
    age = 21,
    tags = Set("dog", "cat"),
    point = newYorkCity,
    _id = id1
  )
  private val p2 = Person(
    name = "Jane Doe",
    age = 19,
    tags = Set("cat"),
    point = noble,
    _id = id2
  )
  private val p3 = Person(
    name = "Bob Dole",
    age = 123,
    tags = Set("dog", "monkey"),
    point = yonkers,
    _id = id3
  )

  specName should {
    "initialize the database" in {
      DB.init().map(b => b should be(true))
    }
    "store John Doe" in {
      DB.people.transaction { implicit transaction =>
        DB.people.set(List(p1, p2, p3)).map { count =>
          count should be(3)
        }
      }
    }
    "query for John Doe" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.filter(_.name === "John Doe").stream.docs.compile.toList.map { people =>
          people.map(_._id) should be(List(id1))
        }
      }
    }
    "truncate the database" in {
      DB.truncate()
    }
    "dispose the database" in {
      DB.dispose()
    }
  }

  protected def storeManager: StoreManager
  protected def indexer(model: Person.type): Indexer[Person, Person.type]

  object DB extends LightDB {
    override lazy val directory: Path = Path.of(s"db/$specName")

    val people: IndexedCollection[Person, Person.type] = collection("people", Person, indexer(Person))

    override def storeManager: StoreManager = spec.storeManager

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String,
                    age: Int,
                    tags: Set[String],
                    point: GeoPoint,
                    _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with Indexed[Person] {
    implicit val rw: RW[Person] = RW.gen

    val name: I[String] = index.one("name", _.name)
    val age: I[Int] = index.one("age", _.age)
    val tag: I[String] = index("tag", _.tags.toList)
    val point: I[GeoPoint] = index.one("point", _.point, sorted = true)
    val search: I[String] = index("search", doc => List(doc.name, doc.age.toString) ::: doc.tags.toList, tokenized = true)
  }
}
