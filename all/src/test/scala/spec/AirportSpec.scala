package spec

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw.RW
import lightdb.halo.HaloDBSupport
import lightdb.lucene.LuceneSupport
import lightdb.{Document, ValueStore, Id, LightDB, Persistence, StoredValue, Unique}
import lightdb.model.Collection
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.nio.file.{Path, Paths}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

class AirportSpec extends AsyncWordSpec with AsyncIOSpec with Matchers {
  "AirportSpec" should {
    "initialize the database" in {
      DB.init(truncate = true)
    }
    "have two collections" in {
      DB.collections.map(_.collectionName).toSet should be(Set("_backingStore", "airports", "flights"))
    }
    "query VIP airports" in {
      Airport.vipKeys.values.map { keys =>
        keys should be(Set("JFK", "ORD", "LAX", "ATL", "AMA", "SFO", "DFW"))
      }
    }
    "query JFK airport" in {
      val jfk = Airport.id("JFK")
      Airport(jfk).map { airport =>
        airport.name should be("John F Kennedy Intl")
      }
    }
    "query the airports by id filter" in {
      val keys = List("JFK", "LAX")
      Airport.query
        .filter(Airport._id IN keys.map(Airport.id))
        .toList
        .map { airports =>
          airports.map(_.name).toSet should be(Set("John F Kennedy Intl", "Los Angeles International"))
        }
    }
    "query by airport name" in {
      Airport.query
        .filter(Airport.name === "John F Kennedy Intl")
        .one
        .map { airport =>
          airport._id should be(Airport.id("JFK"))
        }
    }
    "count all the airports" in {
      Airport.size.map { count =>
        count should be(3375)
      }
    }
    "validate airport references" in {
      Flight.airportReferences.facet(Airport.id("JFK")).map { facet =>
        facet.count should be(4826)
        facet.ids.size should be(4826)
      }
    }
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
      DB.dispose()
    }
  }

  object DB extends LightDB with HaloDBSupport {
    override lazy val directory: Path = Paths.get("airports")

    override lazy val userCollections: List[Collection[_]] = List(
      Airport, Flight
    )

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

  object Airport extends Collection[Airport]("airports", DB) with LuceneSupport[Airport] {
    override implicit val rw: RW[Airport] = RW.gen

    val name: I[String] = index.one[String]("name", _.name)
    val vip: I[Boolean] = index.one[Boolean]("vip", _.vip)
    val vipKeys: ValueStore[String, Airport] = ValueStore[String, Airport]("vipKeys", doc => if (doc.vip) List(doc._id.value) else Nil, this, persistence = Persistence.Cached)

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

  object Flight extends Collection[Flight]("flights", DB) {
    override implicit val rw: RW[Flight] = RW.gen

    val airportReferences: ValueStore[Id[Airport], Flight] = ValueStore[Id[Airport], Flight](
      key = "airportReferences",
      createV = f => List(f.from, f.to),
      collection = this,
      includeIds = true,
      persistence = Persistence.Memory
    )
  }

  object DataImportUpgrade extends DatabaseUpgrade {
    override def applyToNew: Boolean = true
    override def blockStartup: Boolean = true
    override def alwaysRun: Boolean = false

    override def upgrade(db: LightDB): IO[Unit] = for {
      insertedAirports <- {
        val airports = csv2Stream("airports.csv").map { d =>
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
        }
        airports.evalMap(Airport.set(_)).compile.count.map(_.toInt)
      }
      _ = insertedAirports should be(3375)
      insertedFlights <- {
        val flights = csv2Stream("flights.csv").map { d =>
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
        }
        flights.evalMap(Flight.set(_)).compile.count.map(_.toInt)
      }
      _ = insertedFlights should be(286463)
      _ <- DB.commit()
    } yield {
      ()
    }

    def csv2Stream(fileName: String): fs2.Stream[IO, Vector[String]] = {
      val source = Source.fromURL(getClass.getClassLoader.getResource(fileName))
      val iterator = source.getLines()
      iterator.next() // Skip heading
      fs2.Stream.fromIterator[IO](iterator.map { s =>
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
      }, 1000)
    }
  }
}
