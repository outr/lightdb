package spec

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw._
import lightdb._
import lightdb.halo.HaloDBSupport
import lightdb.index.Index
import lightdb.model.Collection
import lightdb.sql.{SQLIndex, SQLSupport}
import lightdb.sqlite.SQLiteSupport
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Path, Paths}

class SimpleHaloAndSQLiteSpec extends AsyncWordSpec with AsyncIOSpec with Matchers {
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
    "verify exactly two objects in index" in {
      Person.index.size.map { size =>
        size should be(2)
      }
    }
    "verify exactly two objects in the store" in {
      Person.idStream.compile.toList.map { ids =>
        ids.toSet should be(Set(id1, id2))
      }
    }
    "search by name for positive result" in {
      Person.withSearchContext { implicit context =>
        Person
          .query
          .countTotal(true)
          .filter(Person.name.is("Jane Doe"))
          .search()
          .flatMap { page =>
            page.page should be(0)
            page.pages should be(1)
            page.offset should be(0)
            page.total should be(1)
            page.ids should be(List(id2))
            page.hasNext should be(false)
            page.docs.map { people =>
              people.length should be(1)
              val p = people.head
              p._id should be(id2)
              p.name should be("Jane Doe")
              p.age should be(19)
            }
          }
      }
    }
    "search by age for positive result" in {
      Person.ageLinks.query(19).compile.toList.map { people =>
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
    "search for age range" in {
      Person.withSearchContext { implicit context =>
        Person
          .query
          .filter(Person.age BETWEEN 19 -> 21)
          .search()
          .flatMap { results =>
            results.docs.map { people =>
              people.length should be(2)
              val names = people.map(_.name).toSet
              names should be(Set("John Doe", "Jane Doe"))
              val ages = people.map(_.age).toSet
              ages should be(Set(21, 19))
            }
          }
      }
    }
    "do paginated search" in {
      Person.withSearchContext { implicit context =>
        Person.query.pageSize(1).countTotal(true).search().flatMap { page1 =>
          page1.page should be(0)
          page1.pages should be(2)
          page1.hasNext should be(true)
          page1.docs.flatMap { people1 =>
            people1.length should be(1)
            page1.next().flatMap {
              case Some(page2) =>
                page2.page should be(1)
                page2.pages should be(2)
                page2.hasNext should be(false)
                page2.docs.map { people2 =>
                  people2.length should be(1)
                }
              case None => fail("Should have a second page")
            }
          }
        }
      }
    }
    "do paginated search as a stream" in {
      Person.withSearchContext { implicit context =>
        Person.query.pageSize(1).countTotal(true).stream.compile.toList.map { people =>
          people.length should be(2)
          people.map(_.name).toSet should be(Set("John Doe", "Jane Doe"))
        }
      }
    }
    "verify the number of records" in {
      Person.index.size.map { size =>
        size should be(2)
      }
    }
    "modify John" in {
      Person.modify(id1) {
        case Some(john) => IO(Some(john.copy(name = "Johnny Doe")))
        case None => throw new RuntimeException("John not found!")
      }.map { person =>
        person.get.name should be("Johnny Doe")
      }
    }
    "commit modified data" in {
      Person.commit()
    }
    "verify the number of records has not changed after modify" in {
      Person.index.size.map { size =>
        size should be(2)
      }
    }
    "verify John was modified" in {
      Person(id1).map { person =>
        person.name should be("Johnny Doe")
      }
    }
    "delete John" in {
      Person.delete(id1).map { deleted =>
        deleted should be(id1)
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
    "verify exactly one object in index" in {
      Person.index.size.map { size =>
        size should be(1)
      }
    }
    "list all documents" in {
      Person.stream.compile.toList.map { people =>
        people.length should be(1)
        val p = people.head
        p._id should be(id2)
        p.name should be("Jane Doe")
        p.age should be(19)
      }
    }
    "replace Jane Doe" in {
      Person.set(Person("Jan Doe", 20, id2)).map { p =>
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
    "dispose" in {
      DB.dispose()
    }
  }

  object DB extends LightDB with HaloDBSupport {
    override lazy val directory: Path = Paths.get("testdb")

    val startTime: StoredValue[Long] = stored[Long]("startTime", -1L)

    override lazy val userCollections: List[Collection[_]] = List(
      Person
    )

    override def upgrades: List[DatabaseUpgrade] = List(InitialSetupUpgrade)
  }

  case class Person(name: String, age: Int, _id: Id[Person] = Id()) extends Document[Person]

  object Person extends Collection[Person]("people", DB) with SQLiteSupport[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: Index[String, Person] = index.one("name", _.name)
    val age: Index[Int, Person] = index.one("age", _.age)
    val ageLinks: IndexedLinks[Int, Person] = indexedLinks[Int]("age", _.toString, _.age)
  }

  object InitialSetupUpgrade extends DatabaseUpgrade {
    override def applyToNew: Boolean = true
    override def blockStartup: Boolean = true
    override def alwaysRun: Boolean = false

    override def upgrade(ldb: LightDB): IO[Unit] = DB.startTime.set(System.currentTimeMillis()).map(_ => ())
  }
}