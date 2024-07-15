package spec

import fabric.rw._
import lightdb.collection.Collection
import lightdb.distance._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.spatial.GeoPoint
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Field, Id, LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Path

abstract class AbstractSpatialSpec extends AnyWordSpec with Matchers { spec =>
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
    point = newYorkCity,
    _id = id1
  )
  private val p2 = Person(
    name = "Jane Doe",
    age = 19,
    point = noble,
    _id = id2
  )
  private val p3 = Person(
    name = "Bob Dole",
    age = 123,
    point = yonkers,
    _id = id3
  )

  specName should {
    "initialize the database" in {
      DB.init() should be(true)
    }
    "store three people" in {
      DB.people.transaction { implicit transaction =>
        DB.people.insert(List(p1, p2, p3)).length should be(3)
      }
    }
    "verify exactly three people exist" in {
      DB.people.transaction { implicit transaction =>
        DB.people.count should be(3)
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
        val distances = list.map(_.distance.mi)
        people.map(_.name) should be(List("Jane Doe", "John Doe"))
        distances should (be (List(28.493883134993137, 1318.8843733311087)) or be(List(28.555356212993576, 1316.1282972648974)))
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

  object DB extends LightDB {
    override lazy val directory: Option[Path] = Some(Path.of(s"db/$specName"))

    val people: Collection[Person, Person.type] = collection(Person)

    override def storeManager: StoreManager = spec.storeManager

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class Person(name: String,
                    age: Int,
                    point: GeoPoint,
                    _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: F[String] = field("name", _.name)
    val age: F[Int] = field("age", _.age)
    val point: I[GeoPoint] = field.index("point", _.point)
  }
}
