package spec

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw._
import lightdb._
import lightdb.halo.HaloDBSupport
import lightdb.lucene.{LuceneIndex, LuceneSupport}
import lightdb.model.Collection
import lightdb.query.Sort
import lightdb.spatial.GeoPoint
import lightdb.upgrade.DatabaseUpgrade
import lightdb.util.DistanceCalculator
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scribe.{Level, Logger}
import squants.space.LengthConversions.LengthConversions

import java.nio.file.{Path, Paths}

class SimpleHaloAndLuceneSpec extends AsyncWordSpec with AsyncIOSpec with Matchers {
  private val id1 = Id[Person]("john")
  private val id2 = Id[Person]("jane")
  private val id3 = Id[Person]("bob")

  private val newYorkCity = GeoPoint(40.7142, -74.0119)
  private val chicago = GeoPoint(41.8119, -87.6873)
  private val jeffersonValley = GeoPoint(41.3385, -73.7947)
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
    "store Jane Doe" in {
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
    "store Bob Dole" in {
      Person.set(p3).map { p =>
        p._id should be(id3)
      }
    }
    "verify Bob Dole exists" in {
      Person.get(id3).map { o =>
        o should be(Some(p3))
      }
    }
    "verify exactly three objects in data" in {
      Person.size.map { size =>
        size should be(3)
      }
    }
    "flush data" in {
      Person.commit()
    }
    "verify exactly three objects in index" in {
      Person.index.size.map { size =>
        size should be(3)
      }
    }
    "verify exactly three objects in the store" in {
      Person.idStream.compile.toList.map { ids =>
        ids.toSet should be(Set(id1, id2, id3))
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
          .filter(Person.age.between(19, 21))
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
    "search by tag" in {
      Person.query.filter(Person.tag === "dog").toList.map { people =>
        people.map(_.name) should be(List("John Doe", "Bob Dole"))
      }
    }
    "do paginated search" in {
      Person.withSearchContext { implicit context =>
        Person.query.pageSize(1).countTotal(true).search().flatMap { page1 =>
          page1.page should be(0)
          page1.pages should be(3)
          page1.hasNext should be(true)
          page1.docs.flatMap { people1 =>
            people1.length should be(1)
            page1.next().flatMap {
              case Some(page2) =>
                page2.page should be(1)
                page2.pages should be(3)
                page2.hasNext should be(true)
                page2.docs.flatMap { people2 =>
                  people2.length should be(1)
                  page2.next().flatMap {
                    case Some(page3) =>
                      page3.page should be(2)
                      page3.pages should be(3)
                      page3.hasNext should be(false)
                      page3.docs.map { people3 =>
                        people3.length should be(1)
                      }
                    case None => fail("Should have a third page")
                  }
                }
              case None => fail("Should have a second page")
            }
          }
        }
      }
    }
    "do paginated search as a stream converting to name" in {
      Person.withSearchContext { implicit context =>
        Person.query.convert(_.name).pageSize(1).countTotal(true).stream.compile.toList.map { names =>
          names.length should be(3)
          names.toSet should be(Set("John Doe", "Jane Doe", "Bob Dole"))
        }
      }
    }
    "sort by age" in {
      Person.query.sort(Sort.ByField(Person.age)).toList.map { people =>
        people.map(_.name) should be(List("Jane Doe", "John Doe", "Bob Dole"))
      }
    }
    "sort by distance from Oklahoma City" in {
      Person.query
        .scoreDocs(true)
        .distance(
          field = Person.point,
          from = oklahomaCity,
          radius = Some(1320.miles)
        )
        .toList
        .map { peopleAndDistances =>
          val people = peopleAndDistances.map(_.doc)
          val distances = peopleAndDistances.map(_.distance)
          people.map(_.name) should be(List("Jane Doe", "John Doe"))
          distances should be(List(28.555228128634383.miles, 1316.1223938032729.miles))
        }
    }
    "search using tokenized data and a parsed query" in {
      Person.query
        .filter(Person.search.words("joh 21"))
        .toList
        .map { results =>
          results.map(_.name) should be(List("John Doe"))
        }
    }
    "delete John" in {
      Person.delete(id1).map { deleted =>
        deleted should be(id1)
      }
    }
    "verify exactly two objects in data again" in {
      Person.size.map { size =>
        size should be(2)
      }
    }
    "commit data" in {
      Person.commit()
    }
    "verify exactly two objects in index again" in {
      Person.index.size.map { size =>
        size should be(2)
      }
    }
    "list all documents" in {
      Person.stream.compile.toList.map { people =>
        people.length should be(2)
        people.map(_._id).toSet should be(Set(id2, id3))
      }
    }
    "replace Jane Doe" in {
      Person.set(Person("Jan Doe", 20, Set("cat", "bear"), chicago, id2)).map { p =>
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
        results.length should be(2)
        results.map(_.name).toSet should be(Set("Jan Doe", "Bob Dole"))
        results.map(_.age).toSet should be(Set(20, 123))
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

    override lazy val collections: List[Collection[_]] = List(
      Person
    )

    override def upgrades: List[DatabaseUpgrade] = List(InitialSetupUpgrade)
  }

  case class Person(name: String,
                    age: Int,
                    tags: Set[String],
                    point: GeoPoint,
                    _id: Id[Person] = Id()) extends Document[Person]

  object Person extends Collection[Person]("people", DB) with LuceneSupport[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: LuceneIndex[String, Person] = index.one("name", _.name)
    val age: LuceneIndex[Int, Person] = index.one("age", _.age)
    val ageLinks: IndexedLinks[Int, Person] = indexedLinks[Int]("age", _.toString, _.age)
    val tag: LuceneIndex[String, Person] = index("tag", _.tags.toList)
    val point: LuceneIndex[GeoPoint, Person] = index.one("point", _.point, sorted = true)
    val search: LuceneIndex[String, Person] = index("search", doc => List(doc.name, doc.age.toString) ::: doc.tags.toList, tokenized = true)
  }

  object InitialSetupUpgrade extends DatabaseUpgrade {
    override def applyToNew: Boolean = true

    override def blockStartup: Boolean = true

    override def alwaysRun: Boolean = false

    override def upgrade(ldb: LightDB): IO[Unit] = DB.startTime.set(System.currentTimeMillis()).map(_ => ())
  }
}