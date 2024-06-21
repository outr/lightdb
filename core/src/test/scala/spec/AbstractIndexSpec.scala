package spec

import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.obj
import fabric.rw.RW
import lightdb.backup.DatabaseBackup
import lightdb.{Id, LightDB}
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.index.{Indexed, IndexedCollection, Indexer}
import lightdb.query.Sort
import lightdb.spatial.GeoPoint
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import squants.space.LengthConversions.LengthConversions

import java.nio.file.Path
import scala.collection.immutable.List

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
    "store three people" in {
      DB.people.transaction { implicit transaction =>
        DB.people.set(List(p1, p2, p3)).map { count =>
          count should be(3)
        }
      }
    }
    "verify exactly three people exist in the index" in {
      DB.people.transaction { implicit transaction =>
        DB.people.indexer.count.map { count =>
          count should be(3)
        }
      }
    }
    "query for John Doe doc" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.filter(_.name === "John Doe").stream.docs.compile.toList.map { people =>
          people.map(_._id) should be(List(id1))
        }
      }
    }
    "query for Jane Doe id" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.filter(_.age === 19).stream.ids.compile.toList.map { ids =>
          ids should be(List(id2))
        }
      }
    }
    "query for Bob Dole materialized" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query
          .filter(_.tag === "monkey")
          .stream
          .materialized(p => List(p.name, p.age))
          .compile
          .toList
          .map { list =>
            list.map(_.json) should be(List(obj(
              "name" -> "Bob Dole",
              "age" -> 123
            )))
          }
      }
    }
    "do a database backup archive" in {
      DatabaseBackup.archive(DB).map { count =>
        count should be(4)
      }
    }
    "search by age range" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.filter(_.age BETWEEN 19 -> 21).stream.ids.compile.toList.map { ids =>
          ids.toSet should be(Set(id1, id2))
        }
      }
    }
    "sort by age" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.sort(Sort.ByIndex(Person.age).descending).stream.docs.compile.toList.map { people =>
          people.map(_.name) should be(List("Bob Dole", "John Doe", "Jane Doe"))
        }
      }
    }
    "group by age" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.grouped(_.age).compile.toList.map { list =>
          list.map(_._1) should be(List(19, 21, 123))
          list.map(_._2.toList.map(_.name)) should be(List(List("Jane Doe"), List("John Doe"), List("Bob Dole")))
        }
      }
    }
    "sort by distance from Oklahoma City" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.stream.distance(
          _.point,
          from = oklahomaCity,
          radius = Some(1320.miles)
        ).compile.toList.map { list =>
          val people = list.map(_.doc)
          val distances = list.map(_.distance)
          people.map(_.name) should be(List("Jane Doe", "John Doe"))
          distances should be(List(28.555228128634383.miles, 1316.1223938032729.miles))
        }
      }
    }
    "search using tokenized data and a parsed query" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.filter(_.search.words("joh 21")).stream.docs.compile.toList.map { list =>
          list.map(_.name) should be(List("John Doe"))
        }
      }
    }
    "find Jane by parsed query" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.filter(_._id.parsed(id2.value)).stream.docs.compile.toList.map { list =>
          list.map(_.name) should be(List("Jane Doe"))
        }
      }
    }
    "delete Jane" in {
      DB.people.transaction { implicit transaction =>
        DB.people.delete(p2).map {
          case Some(p) => p.name should be("Jane Doe")
          case None => fail()
        }
      }
    }
    "verify John and Bob still exist" in {
      DB.people.transaction { implicit transaction =>
        DB.people.query.stream.docs.compile.toList.map { people =>
          people.map(_.name).toSet should be(Set("John Doe", "Bob Dole"))
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

    val name: I[String] = index.one("name", _.name, store = true)
    val age: I[Int] = index.one("age", _.age, store = true)
    val tag: I[String] = index("tag", _.tags.toList)
    val point: I[GeoPoint] = index.one("point", _.point, sorted = true)
    val search: I[String] = index("search", doc => List(doc.name, doc.age.toString) ::: doc.tags.toList, tokenized = true)
  }
}
