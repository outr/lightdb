package spec

import fabric.rw._
import lightdb.collection.Collection
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.halodb.HaloDBStore
import lightdb.lucene.LuceneStore
import lightdb.store.StoreManager
import lightdb.store.split.SplitStoreManager
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Id, LightDB, Unique}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task}
import scribe.{rapid => logger}

import java.nio.file.Path
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

@EmbeddedTest
class AirportSpec extends AsyncWordSpec with AsyncTaskSpec with Matchers {
  "AirportSpec" should {
    "initialize the database" in {
      DB.init.succeed
    }
    "have two collections" in {
      DB.collections.map(_.name).toSet should be(Set("_backingStore", "Flight", "Airport"))
      Task.unit.succeed
    }
//    "query VIP airports" in {
//      Airport.vipKeys.values.map { keys =>
//        keys should be(Set("JFK", "ORD", "LAX", "ATL", "AMA", "SFO", "DFW"))
//      }
//    }
    "query JFK airport" in {
      val jfk = Airport.id("JFK")
      DB.airports.t(jfk).map { airport =>
        airport.name should be("John F Kennedy Intl")
      }
    }
    "query the airports by id filter" in {
      val keys = List("JFK", "LAX")
      DB.airports.transaction { implicit transaction =>
        DB.airports.query
          .filter(_._id IN keys.map(Airport.id))
          .toList
          .map { airports =>
            airports.map(_.name).toSet should be(Set("John F Kennedy Intl", "Los Angeles International"))
          }
      }
    }
    "query by airport name" in {
      DB.airports.transaction { implicit transaction =>
        DB.airports.query
          .filter(_.name === "John F Kennedy Intl")
          .first
          .map { airport =>
            airport._id should be(Airport.id("JFK"))
          }
      }
    }
    "count all the airports" in {
      DB.airports.t.count.map(_ should be(3375))
    }
//    "validate airport references" in {
//      Flight.airportReferences.facet(Airport.id("JFK")).map { facet =>
//        facet.count should be(4826)
//        facet.ids.size should be(4826)
//      }
//    }
    // TODO: Support traversals
    /*"get all airport names reachable directly from LAX following edges" in {
      val lax = Airport.id("LAX")
      val query =
        aql"""
             FOR airport IN 1..1 OUTBOUND $lax ${database.flights}
             RETURN DISTINCT airport.name
           """
      database.query[String](query).toList.map { response =>
        response.length should be(82)
      }
    }
    "traverse all airports reachable from LAX" in {
      val lax = Airport.id("LAX")
      val query =
        aql"""
             FOR airport IN OUTBOUND $lax ${database.flights}
             OPTIONS { bfs: true, uniqueVertices: 'global' }
             RETURN airport
           """
      database.airports.query(query).toList.map { response =>
        response.length should be(82)
      }
    }
    "find the shortest path between BIS and JFK" in {
      val bis = Airport.id("BIS")
      val jfk = Airport.id("JFK")
      val query =
        aql"""
             FOR v IN OUTBOUND
             SHORTEST_PATH $bis TO $jfk ${database.flights}
             RETURN v.${Airport.name}
           """
      database.query[String](query).toList.map { response =>
        response should be(List("Bismarck Municipal", "Minneapolis-St Paul Intl", "John F Kennedy Intl"))
      }
    }*/
    // TODO: Test ValueStore
    // TODO: the other stuff
    "dispose" in {
      DB.dispose().succeed
    }
  }

  object DB extends LightDB {
    override def storeManager: StoreManager = SplitStoreManager(HaloDBStore, LuceneStore)

    lazy val directory: Option[Path] = Some(Path.of("db/AirportSpec"))

    val airports: Collection[Airport, Airport.type] = collection(Airport)
    val flights: Collection[Flight, Flight.type] = collection(Flight)

    override def upgrades: List[DatabaseUpgrade] = List(DataImportUpgrade)
  }

  case class Airport(name: String,
                     city: String,
                     state: String,
                     country: String,
                     lat: Double,
                     long: Double,
                     vip: Boolean,
                     _id: Id[Airport] = Airport.id()) extends Document[Airport]

  object Airport extends DocumentModel[Airport] with JsonConversion[Airport] {
    override implicit val rw: RW[Airport] = RW.gen

    val name: I[String] = field.index("name", _.name)
    val city: F[String] = field("city", _.city)
    val state: F[String] = field("state", _.state)
    val country: F[String] = field("country", _.country)
    val lat: F[Double] = field("lat", _.lat)
    val long: F[Double] = field("long", _.long)
    val vip: I[Boolean] = field.index("vip", _.vip)
//    val vipKeys: ValueStore[String, Airport] = ValueStore[String, Airport]("vipKeys", doc => if (doc.vip) List(doc._id.value) else Nil, this, persistence = Persistence.Cached)

    override def id(value: String = Unique()): Id[Airport] = {
      val index = value.indexOf('/')
      val v = if (index != -1) {
        value.substring(index + 1)
      } else {
        value
      }
      Id(v)
    }
  }

  case class Flight(from: Id[Airport],
                    to: Id[Airport],
                    year: Int,
                    month: Int,
                    day: Int,
                    dayOfWeek: Int,
                    depTime: Int,
                    arrTime: Int,
                    depTimeUTC: String,
                    arrTimeUTC: String,
                    uniqueCarrier: String,
                    flightNum: Int,
                    tailNum: String,
                    distance: Int,
                    _id: Id[Flight] = Flight.id()) extends Document[Flight]

  object Flight extends DocumentModel[Flight] with JsonConversion[Flight] {
    override implicit val rw: RW[Flight] = RW.gen

    val from: F[Id[Airport]] = field("from", _.from)
    val to: F[Id[Airport]] = field("to", _.to)
    val year: F[Int] = field("year", _.year)
    val month: F[Int] = field("month", _.month)
    val day: F[Int] = field("day", _.day)
    val dayOfWeek: F[Int] = field("dayOfWeek", _.dayOfWeek)
    val depTime: F[Int] = field("depTime", _.depTime)
    val arrTime: F[Int] = field("arrTime", _.arrTime)
    val depTimeUTC: F[String] = field("depTimeUTC", _.depTimeUTC)
    val arrTimeUTC: F[String] = field("arrTimeUTC", _.arrTimeUTC)
    val uniqueCarrier: F[String] = field("uniqueCarrier", _.uniqueCarrier)
    val flightNum: F[Int] = field("flightNum", _.flightNum)
    val tailNum: F[String] = field("tailNum", _.tailNum)
    val distance: F[Int] = field("distance", _.distance)

//    val airportReferences: ValueStore[Id[Airport], Flight] = ValueStore[Id[Airport], Flight](
//      key = "airportReferences",
//      createV = f => List(f.from, f.to),
//      collection = this,
//      includeIds = true,
//      persistence = Persistence.Memory
//    )
  }

  object DataImportUpgrade extends DatabaseUpgrade {
    override def applyToNew: Boolean = true
    override def blockStartup: Boolean = true
    override def alwaysRun: Boolean = false

    def csv2Iterator(fileName: String): Iterator[Vector[String]] = {
      val source = Source.fromURL(getClass.getClassLoader.getResource(fileName))
      val iterator = source.getLines()
      iterator.next() // Skip heading
      iterator.map { s =>
        var open = false
        val entries = ListBuffer.empty[String]
        val b = new mutable.StringBuilder
        s.foreach { c =>
          if (c == '"') {
            open = !open
          } else if (c == ',' && !open) {
            if (b.nonEmpty) {
              entries += b.toString().trim
              b.clear()
            }
          } else {
            b.append(c)
          }
        }
        if (b.nonEmpty) entries += b.toString().trim
        entries.toVector
      }
    }

    override def upgrade(db: LightDB): Task[Unit] = for {
      _ <- logger.info("Data Importing...")
      airports = rapid.Stream.fromIterator(Task(csv2Iterator("airports.csv").map { d =>
        Airport(
          name = d(1),
          city = d(2),
          state = d(3),
          country = d(4),
          lat = d(5).toDouble,
          long = d(6).toDouble,
          vip = d(7).toBoolean,
          _id = Airport.id(d(0))
        )
      }))
      insertedAirports <- DB.airports.transaction { implicit transaction =>
        airports
          .evalForeach { airport =>
            DB.airports.insert(airport).unit
          }
          .count
      }
      _ = insertedAirports should be(3375)
      flights = rapid.Stream.fromIterator(Task(csv2Iterator("flights.csv").map { d =>
        Flight(
          from = Airport.id(d(0)),
          to = Airport.id(d(1)),
          year = d(2).toInt,
          month = d(3).toInt,
          day = d(4).toInt,
          dayOfWeek = d(5).toInt,
          depTime = d(6).toInt,
          arrTime = d(7).toInt,
          depTimeUTC = d(8),
          arrTimeUTC = d(9),
          uniqueCarrier = d(10),
          flightNum = d(11).toInt,
          tailNum = d(12),
          distance = d(13).toInt
        )
      }))
      insertedFlights <- DB.flights.transaction { implicit transaction =>
        flights
          .evalForeach { flight =>
            DB.flights.insert(flight).unit
          }
          .count
      }
      _ = insertedFlights should be(286463)
    } yield ()
  }
}
