package spec

import fabric._
import fabric.rw._
import lightdb.backup.{DatabaseBackup, DatabaseRestore}
import lightdb.{Id, LightDB, StoredValue}
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.index.{Indexed, IndexedCollection, Indexer}
import lightdb.query.Sort
import lightdb.spatial.GeoPoint
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}
import squants.space.LengthConversions.LengthConversions

import java.nio.file.Path
import scala.collection.immutable.List

abstract class AbstractIndexAndSpatialSpec extends AnyWordSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val id1 = Id[Person]("john")
  private val id2 = Id[Person]("jane")
  private val id3 = Id[Person]("bob")

  private val newYorkCity = GeoPoint(40.7142, -74.0119)
  private val chicago = GeoPoint(41.8119, -87.6873)
  private val noble = GeoPoint(35.1417, -97.3409)
  private val oklahomaCity = GeoPoint(35.5514, -97.4075)
  private val yonkers = GeoPoint(40.9461, -73.8669)

  protected def supportsAggregateFunctions: Boolean = true

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
      DB.init() should be(true)
    }
    "store three people" in {
      DB.people.transaction { implicit transaction =>
        DB.people.set(List(p1, p2, p3)) should be(3)
      }
    }
    "verify exactly three people exist in the index" in {
      DB.people.transaction { implicit transaction =>
        DB.people.indexer.count should be(3)
      }
    }
    "query for John Doe doc" in {
      DB.people.transaction { implicit transaction =>
        val people = DB.people.query.filter(_.name === "John Doe").search.docs.iterator.toList
        people.map(_._id) should be(List(id1))
      }
    }
    "query for Jane Doe id" in {
      DB.people.transaction { implicit transaction =>
        val ids = DB.people.query.filter(_.age === 19).search.ids.iterator.toList
        ids should be(List(id2))
      }
    }
    "query for Bob Dole materialized" in {
      DB.people.transaction { implicit transaction =>
        val list = DB.people.query
          .filter(_.tag === "monkey")
          .search
          .materialized(p => List(p.name, p.age))
          .iterator
          .toList
        list.map(_.json) should be(List(obj(
          "name" -> "Bob Dole",
          "age" -> 123
        )))
        list.map(_(_.age)) should be(List(123))
      }
    }
    "query with aggregate functions" in {
      if (supportsAggregateFunctions) {
        DB.people.transaction { implicit transaction =>
          val list = DB.people.query
            .aggregate(p => List(
              p.age.min,
              p.age.max,
              p.age.avg,
              p.age.sum
            ))
            .toList
          list.map(m => m(_.age.min)).toSet should be(Set(19))
          list.map(m => m(_.age.max)).toSet should be(Set(21))
          list.map(m => m(_.age.avg)).toSet should be(Set(20.0))
          list.map(m => m(_.age.sum)).toSet should be(Set(40.0))
        }
      } else {
        succeed
      }
    }
    "do a database backup archive" in {
      DatabaseBackup.archive(DB) should be(6)
    }
    "search by age range" in {
      DB.people.transaction { implicit transaction =>
        val ids = DB.people.query.filter(_.age BETWEEN 19 -> 21).search.ids.iterator.toList
        ids.toSet should be(Set(id1, id2))
      }
    }
    "sort by age" in {
      DB.people.transaction { implicit transaction =>
        val people = DB.people.query.sort(Sort.ByIndex(Person.age).descending).search.docs.iterator.toList
        people.map(_.name) should be(List("Bob Dole", "John Doe", "Jane Doe"))
      }
    }
    "group by age" in {
      DB.people.transaction { implicit transaction =>
        val list = DB.people.query.grouped(_.age).toList
        list.map(_._1) should be(List(19, 21, 123))
        list.map(_._2.toList.map(_.name)) should be(List(List("Jane Doe"), List("John Doe"), List("Bob Dole")))
      }
    }
    "sort by distance from Oklahoma City" in {
      DB.people.transaction { implicit transaction =>
        val list = DB.people.query.search.distance(
          _.point,
          from = oklahomaCity,
          radius = Some(1320.miles)
        ).iterator.toList
        val people = list.map(_.doc)
        val distances = list.map(_.distance)
        people.map(_.name) should be(List("Jane Doe", "John Doe"))
        distances should be(List(28.555228128634383.miles, 1316.1223938032729.miles))
      }
    }
    "replace Jane Doe" in {
      DB.people.transaction { implicit transaction =>
        DB.people.set(Person("Jan Doe", 20, Set("cat", "bear"), chicago, id2)) match {
          case Some(p) =>
            p._id should be(id2)
            p.name should be("Jan Doe")
          case None => fail()
        }
      }
    }
    "search using tokenized data and a parsed query" in {
      DB.people.transaction { implicit transaction =>
        val list = DB.people.query.filter(_.search.words("joh 21")).search.docs.iterator.toList
        list.map(_.name) should be(List("John Doe"))
      }
    }
    "find Jan by parsed query" in {
      DB.people.transaction { implicit transaction =>
        val list = DB.people.query.filter(_._id.parsed(id2.value)).search.docs.iterator.toList
        list.map(_.name) should be(List("Jan Doe"))
      }
    }
    "delete Jane" in {
      DB.people.transaction { implicit transaction =>
        DB.people.delete(id2) match {
          case Some(p) => p.name should be("Jan Doe")
          case None => fail()
        }
      }
    }
    "verify John and Bob still exist" in {
      DB.people.transaction { implicit transaction =>
        val people = DB.people.query.search.docs.iterator.toList
        people.map(_.name).toSet should be(Set("John Doe", "Bob Dole"))
      }
    }
    "verify start time has been set" in {
      DB.startTime.get()  should be > 0L
    }
    "restore from the database backup" in {
      val count = DatabaseRestore.archive(DB)
      count should be(6)
    }
    "verify Jane has been restored" in {
      DB.people.transaction { implicit transaction =>
        val people = DB.people.query.search.docs.iterator.toList
        people.map(_.name).toSet should be(Set("John Doe", "Bob Dole", "Jane Doe"))
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
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val startTime: StoredValue[Long] = stored[Long]("startTime", -1L)

    val people: IndexedCollection[Person, Person.type] = collection("people", Person, indexer(Person))

    override def storeManager: StoreManager = spec.storeManager

    override def upgrades: List[DatabaseUpgrade] = List(InitialSetupUpgrade)
  }

  case class Person(name: String,
                    age: Int,
                    tags: Set[String],
                    point: GeoPoint,
                    _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with Indexed[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: I[String] = index.one("name", _.name, store = true)
    val age: I[Int] = index.one("age", _.age, store = true)
    val tag: I[String] = index("tag", _.tags.toList)
    val point: I[GeoPoint] = index.one("point", _.point, sorted = true)
    val search: I[String] = index("search", doc => List(doc.name, doc.age.toString) ::: doc.tags.toList, tokenized = true)
  }

  object InitialSetupUpgrade extends DatabaseUpgrade {
    override def applyToNew: Boolean = true

    override def blockStartup: Boolean = true

    override def alwaysRun: Boolean = false

    override def upgrade(ldb: LightDB): Unit = DB.startTime.set(System.currentTimeMillis())
  }
}
