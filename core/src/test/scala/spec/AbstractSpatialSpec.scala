package spec

import fabric.io.JsonParser
import fabric.rw._
import lightdb.collection.Collection
import lightdb.distance._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.field.Field
import lightdb.spatial.Geo
import lightdb.store.StoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Id, LightDB}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Path

abstract class AbstractSpatialSpec extends AnyWordSpec with Matchers { spec =>
  private lazy val specName: String = getClass.getSimpleName

  private val id1 = Id[Person]("john")
  private val id2 = Id[Person]("jane")
  private val id3 = Id[Person]("bob")

  private val newYorkCity = Geo.Point(40.7142, -74.0119)
  private val chicago = Geo.Point(41.8119, -87.6873)
  private val noble = Geo.Point(35.1417, -97.3409)
  private val oklahomaCity = Geo.Point(35.5514, -97.4075)
  private val yonkers = Geo.Point(40.9461, -73.8669)

  private val moorePolygon = Geo.Polygon.lonLat(
    -97.51995284659067, 35.31659661477283,
    -97.50983688600051, 35.29708140953622,
    -97.42966767585344, 35.29494585205129,
    -97.41303352647198, 35.31020363480967,
    -97.41331385837709, 35.34926895467585,
    -97.42803670547956, 35.36508604748108,
    -97.50690451974124, 35.36587866914906,
    -97.51755160616675, 35.35131024794894,
    -97.51995284659067, 35.31659661477283
  )

  protected def supportsAggregateFunctions: Boolean = true

  private val p1 = Person(
    name = "John Doe",
    age = 21,
    point = newYorkCity,
    geo = Nil,
    _id = id1
  )
  private val p2 = Person(
    name = "Jane Doe",
    age = 19,
    point = noble,
    geo = List(moorePolygon),
    _id = id2
  )
  private val p3 = Person(
    name = "Bob Dole",
    age = 123,
    point = yonkers,
    geo = List(chicago, yonkers),
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
          _.point.list,
          from = oklahomaCity,
          radius = Some(1320.miles)
        ).iterator.toList
        val people = list.map(_.doc)
        val distances = list.map(_.distance.map(_.mi.toInt))
        people.zip(distances).map {
          case (p, d) => p.name -> d
        } should be(List(
          "Jane Doe" -> List(28),
          "John Doe" -> List(1316)
        ))
      }
    }
    "sort by distance from Noble using geo" in {
      DB.people.transaction { implicit transaction =>
        val list = DB.people.query.search.distance(
          _.geo,
          from = noble,
          radius = Some(10_000.miles)
        ).iterator.toList
        val people = list.map(_.doc)
        val distances = list.map(_.distance.map(_.mi))
        people.zip(distances).map {
          case (p, d) => p.name -> d
        } should be(List(
          "Jane Doe" -> List(16.01508397712445),
          "Bob Dole" -> List(695.6419047674393, 1334.038796028706)
        ))
      }
    }
    "parse and insert from a GeometryCollection" in {
      DB.people.transaction { implicit transaction =>
        val json = JsonParser("""{"crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4269"}}, "type": "GeometryCollection", "geometries": [{"type": "LineString", "coordinates": [[-103.79558, 32.30492], [-103.793467263, 32.331700182]]}]}""")
        val geo = Geo.parseMulti(json)
        DB.people.insert(Person(
          name = "Baby Dole",
          age = 2,
          point = yonkers,
          geo = geo
        ))
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
                    point: Geo.Point,
                    geo: List[Geo],
                    _id: Id[Person] = Person.id()) extends Document[Person]

  object Person extends DocumentModel[Person] with JsonConversion[Person] {
    override implicit val rw: RW[Person] = RW.gen

    val name: F[String] = field("name", (p: Person) => p.name)
    val age: F[Int] = field("age", (p: Person) => p.age)
    val point: I[Geo.Point] = field.index("point", (p: Person) => p.point)
    val geo: I[List[Geo]] = field.index("geo", (p: Person) => p.geo)
  }
}
