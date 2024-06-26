package spec

import fabric.obj
import fabric.rw.RW
import lightdb.backup.{DatabaseBackup, DatabaseRestore}
import lightdb.document.{Document, DocumentModel}
import lightdb.index.{Indexed, IndexedCollection, Indexer}
import lightdb.query.Sort
import lightdb.spatial.GeoPoint
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Id, LightDB, StoredValue}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}
import squants.space.LengthConversions.LengthConversions

import java.nio.file.Path

abstract class AbstractIndexSpec extends AnyWordSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val id1 = Id[Person]("john")
  private val id2 = Id[Person]("jane")
  private val id3 = Id[Person]("bob")

  protected def supportsAggregateFunctions: Boolean = true
  protected def supportsParsed: Boolean = true

  private val p1 = Person(
    name = "John Doe",
    age = 21,
    tags = Set("dog", "cat"),
    _id = id1
  )
  private val p2 = Person(
    name = "Jane Doe",
    age = 19,
    tags = Set("cat"),
    _id = id2
  )
  private val p3 = Person(
    name = "Bob Dole",
    age = 123,
    tags = Set("dog", "monkey"),
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
        val ids = DB.people.query.filter(_.age === 19).search.ids.list
        ids should be(List(id2))
      }
    }
    "query for Bob Dole materialized" in {
      DB.people.transaction { implicit transaction =>
        val list = DB.people.query
          .filter(_.name === "Bob Dole")
          .search
          .materialized(p => List(p.name, p.age))
          .list
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
        val ids = DB.people.query.filter(_.age BETWEEN 19 -> 21).search.ids.list
        ids.toSet should be(Set(id1, id2))
      }
    }
    "sort by age" in {
      DB.people.transaction { implicit transaction =>
        val people = DB.people.query.sort(Sort.ByIndex(Person.age).descending).search.docs.list
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
    "replace Jane Doe" in {
      DB.people.transaction { implicit transaction =>
        DB.people.set(p2.copy(
          name = "Jan Doe",
          age = 20,
          tags = Set("cat", "bear")
        )) match {
          case Some(p) =>
            p._id should be(id2)
            p.name should be("Jan Doe")
          case None => fail()
        }
      }
    }
    "search using tokenized data and a parsed query" in {
      if (supportsParsed) {
        DB.people.transaction { implicit transaction =>
          val list = DB.people.query.filter(_.search.words("joh 21")).search.docs.list
          list.map(_.name) should be(List("John Doe"))
        }
      } else {
        succeed
      }
    }
    "find Jan by parsed query" in {
      if (supportsParsed) {
        DB.people.transaction { implicit transaction =>
          val list = DB.people.query.filter(_._id.parsed(id2.value)).search.docs.list
          list.map(_.name) should be(List("Jan Doe"))
        }
      } else {
        succeed
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
        val people = DB.people.query.search.docs.list
        people.map(_.name).toSet should be(Set("John Doe", "Bob Dole"))
      }
    }
    "verify start time has been set" in {
      DB.startTime.get() should be > 0L
    }
    "restore from the database backup" in {
      DatabaseRestore.archive(DB) should be(6)
    }
    "verify Jane has been restored" in {
      DB.people.transaction { implicit transaction =>
        val people = DB.people.query.search.docs.list
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
                    _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with Indexed[Person] {
    implicit val rw: RW[Person] = RW.gen

    val name: I[String] = index.one("name", _.name, store = true)
    val age: I[Int] = index.one("age", _.age, store = true)
    val tag: I[String] = index("tag", _.tags.toList)
    val search: I[String] = index("search", doc => List(doc.name, doc.age.toString) ::: doc.tags.toList, tokenized = true)
  }

  object InitialSetupUpgrade extends DatabaseUpgrade {
    override def applyToNew: Boolean = true

    override def blockStartup: Boolean = true

    override def alwaysRun: Boolean = false

    override def upgrade(ldb: LightDB): Unit = DB.startTime.set(System.currentTimeMillis())
  }
}
