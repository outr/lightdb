package spec

import fabric.rw._
import lightdb._
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.graph.{EdgeDocument, EdgeModel}
import lightdb.id.{EdgeId, Id}
import lightdb.lucene.LuceneStore
import lightdb.rocksdb.RocksDBStore
import lightdb.store.split.SplitStoreManager
import lightdb.traverse.TraversalPath
import lightdb.upgrade.DatabaseUpgrade
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import rapid.{AsyncTaskSpec, Task, Unique, logger}

import java.nio.file.Path
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
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
      DB.stores.map(_.name).toSet should be(Set("_backingStore", "Flight", "Airport"))
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
      DB.airports.transaction { transaction =>
        transaction.query
          .filter(_._id.in(keys.map(Airport.id)))
          .toList
          .map { airports =>
            airports.map(_.name).toSet should be(Set("John F Kennedy Intl", "Los Angeles International"))
          }
      }
    }
    "query by airport name" in {
      DB.airports.transaction { transaction =>
        transaction.query
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
    "count all connections" in {
      DB.flights.t.count.map(_ should be(286463))
    }
    "count all connections to JFK" in {
      DB.flights.transaction { tx =>
        tx.storage.traverse.edgesFor[Flight, Airport, Airport](Airport.id("JFK")).toList.map { flights =>
          flights.length should be(4806)
          flights.map(_._to.value).toSet should be(Set(
            "LAS", "LAX", "MIA", "SJC", "HDN", "CMH", "PIT", "DCA", "IAD", "FLL", "CLE", "SYR", "STL", "LGB", "ORD",
            "BUR", "PVD", "STT", "BQN", "RIC", "ATL", "CLT", "BTV", "SEA", "PHX", "SMF", "BUF", "IAH", "ORF", "SRQ",
            "PDX", "MSP", "CVG", "SAN", "TPA", "SFO", "AUS", "TUS", "RDU", "ROC", "PHL", "PWM", "OAK", "JAX", "SJU",
            "DFW", "PBI", "BNA", "DTW", "MCO", "DEN", "MSY", "BOS", "PSE", "IND", "HPN", "BWI", "ONT", "BDL", "HOU",
            "RSW", "SLC"
          ))
        }
      }
    }
    "get all airport names reachable directly from LAX following edges" in {
      DB.flights.transaction { tx =>
        val lax = Airport.id("LAX")
        tx.storage.traverse.edgesFor[Flight, Airport, Airport](lax).toList.map { flights =>
          flights.map(_._to.value).distinct.length should be(82)
        }
      }
    }
    "get all airports reachable from LAX" in {
      val lax = Airport.id("LAX")
      DB.flights.transaction { tx =>
        tx.storage.traverse.reachableFrom[Flight, Airport, Airport](lax).toList.map { flights =>
          flights.length should be(286463)
          val airports = flights.map(_._to.value).toSet
          airports.size should be(286)
        }
      }
    }
    "find the shortest paths between BIS and JFK" in {
      val bis = Airport.id("BIS")
      val jfk = Airport.id("JFK")
      DB.flights.transaction { tx =>
        tx.storage.traverse.shortestPaths[Flight, Airport, Airport](bis, jfk).map { path =>
          path.nodes.map(_.value).mkString(" -> ")
        }.distinct.toList.map { list =>
          list.toSet should be(Set("BIS -> DEN -> JFK", "BIS -> MSP -> JFK"))
        }
      }
    }
    "find the shortest paths between BIS and JFK with between an hour and eight between each hop" in {
      val bis = Airport.id("BIS")
      val jfk = Airport.id("JFK")

      DB.flights.transaction { tx =>
        tx.storage.traverse.shortestPaths[Flight, Airport, Airport](
          from = bis,
          to = jfk,
          edgeFilter = _ => true
        ).filter { path =>
          path.edges.sliding(2).forall {
            case List(f1, f2) =>
              val gap = f2.departure - f1.arrival
              gap >= 3600 * 1000 && gap <= 3600 * 8000
            case _ => true
          }
        }.map(path => Trip(path)).toList.map { trips =>
          trips.minBy(_.totalMillis)
        }.map { trip =>
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneOffset.UTC)

          val hops = trip.path.edges.map { f =>
            val dep = formatter.format(Instant.ofEpochMilli(f.departure))
            val arr = formatter.format(Instant.ofEpochMilli(f.arrival))
            s"${f._from} -> ${f._to} [$dep - $arr]"
          }.mkString(" | ")


          scribe.info(s"Path: $hops | Total trip time: ${trip.totalHours}h ${trip.totalMinutes}m")
          trip.totalMillis should be(16980000L)
        }
      }
    }
    "find all paths between BIS and JFK" in {
      val bis = Airport.id("BIS")
      val jfk = Airport.id("JFK")
      val maxPaths = 1_000
      DB.flights.transaction { tx =>
        tx.storage.traverse.allPaths[Flight, Airport](bis, jfk, maxDepth = 100).take(maxPaths).toList.map { paths =>
          paths.length should be(maxPaths)
        }
      }
    }
    "use traverse.bfs for airport connections" in {
      val lax = Airport.id("LAX")

      DB.flights.transaction { tx =>
        for {
          // Get all direct connections from LAX (1-hop)
          directConnections <- tx.storage.traverse.edgesFor[Flight, Airport, Airport](lax)
            .map(_._to)
            .distinct
            .toList
        } yield {
          // Verify direct connections from LAX
          directConnections.length should be > 50
          directConnections should contain(Airport.id("SFO"))
          directConnections should contain allOf(
            Airport.id("JFK"),
            Airport.id("ORD"),
            Airport.id("DFW")
          )
        }
      }
    }
    "find connections up to two hops away" in {
      val bis = Airport.id("BIS")
      val jfk = Airport.id("JFK")

      DB.flights.transaction { tx =>
        for {
          // Get all airports reachable from BIS in 1 hop
          oneHopConnections <- tx.storage.traverse.edgesFor[Flight, Airport, Airport](bis)
            .map(_._to)
            .toList
          // From those airports, get airports reachable in 1 more hop (total: 2 hops)
          twoHopConnections <- Task.sequence(oneHopConnections.map { airport =>
            tx.storage.traverse.edgesFor[Flight, Airport, Airport](airport)
              .map(_._to)
              .toList
          }).map(_.flatten)
          // Combine 1-hop and 2-hop destinations
          allReachable = (oneHopConnections ++ twoHopConnections).distinct
        } yield {
          // JFK should be reachable from BIS within 2 hops
          allReachable should contain(jfk)

          // Verify intermediate connections
          allReachable should contain allOf(
            Airport.id("DEN"),
            Airport.id("MSP")
          )
        }
      }
    }
    "compare new traverse with BFS traversal" in {
      val lax = Airport.id("LAX")

      DB.flights.transaction { tx =>
        val reachable = traverse
          .from(lax)
          .follow[Flight, Airport](tx.storage)
          .targetIds
          .toList

        val bfs = tx.storage.traverse
          .bfs[Flight, Airport, Airport](lax)
          .follow[Flight, Airport](tx.storage)
          .targetIds
          .toList

        for {
          viaTraverse <- reachable
          viaBfs <- bfs.map(_.toSet)
        } yield {
          val viaTraverseSet = viaTraverse.toSet
          viaTraverseSet should be(viaBfs)
          viaTraverseSet.size should be(286)
        }
      }
    }
    "dispose" in {
      DB.dispose.succeed
    }
  }

  object DB extends LightDB {
    override type SM = SplitStoreManager[RocksDBStore.type, LuceneStore.type]
    override val storeManager: SplitStoreManager[RocksDBStore.type, LuceneStore.type] = SplitStoreManager(RocksDBStore, LuceneStore)

    override lazy val directory: Option[Path] = Some(Path.of("db/AirportSpec"))

    val airports: S[Airport, Airport.type] = store(Airport)
    val flights: S[Flight, Flight.type] = store(Flight)

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

    override def id(value: String = Unique.sync()): Id[Airport] = {
      val index = value.indexOf('/')
      val v = if (index != -1) {
        value.substring(index + 1)
      } else {
        value
      }
      Id(v)
    }
  }

  case class Flight(_from: Id[Airport],
                    _to: Id[Airport],
                    year: Int,
                    month: Int,
                    day: Int,
                    dayOfWeek: Int,
                    depTime: Int,
                    arrTime: Int,
                    departure: Long,
                    arrival: Long,
                    uniqueCarrier: String,
                    flightNum: Int,
                    tailNum: String,
                    distance: Int,
                    _id: EdgeId[Flight, Airport, Airport]) extends EdgeDocument[Flight, Airport, Airport]

  object Flight extends EdgeModel[Flight, Airport, Airport] with JsonConversion[Flight] {
    override implicit val rw: RW[Flight] = RW.gen

    def apply(_from: Id[Airport],
              _to: Id[Airport],
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
              distance: Int): Flight = Flight(
      _from = _from,
      _to = _to,
      year = year,
      month = month,
      day = day,
      dayOfWeek = dayOfWeek,
      depTime = depTime,
      arrTime = arrTime,
      departure = Instant.parse(depTimeUTC).toEpochMilli,
      arrival = Instant.parse(arrTimeUTC).toEpochMilli,
      uniqueCarrier = uniqueCarrier,
      flightNum = flightNum,
      tailNum = tailNum,
      distance = distance,
      _id = EdgeId(_from, _to, Unique())
    )

    val year: F[Int] = field("year", _.year)
    val month: F[Int] = field("month", _.month)
    val day: F[Int] = field("day", _.day)
    val dayOfWeek: F[Int] = field("dayOfWeek", _.dayOfWeek)
    val depTime: F[Int] = field("depTime", _.depTime)
    val arrTime: F[Int] = field("arrTime", _.arrTime)
    val departure: F[Long] = field("departure", _.departure)
    val arrival: F[Long] = field("arrival", _.arrival)
    val uniqueCarrier: F[String] = field("uniqueCarrier", _.uniqueCarrier)
    val flightNum: F[Int] = field("flightNum", _.flightNum)
    val tailNum: F[String] = field("tailNum", _.tailNum)
    val distance: F[Int] = field("distance", _.distance)
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
      insertedAirports <- DB.airports.transaction { transaction =>
        airports
          .evalForeach { airport =>
            transaction.insert(airport).unit
          }
          .count
      }
      _ = insertedAirports should be(3375)
      flights = rapid.Stream.fromIterator(Task(csv2Iterator("flights.csv").map { d =>
        Flight(
          _from = Airport.id(d(0)),
          _to = Airport.id(d(1)),
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
      insertedFlights <- DB.flights.transaction { transaction =>
        flights
          .evalForeach { flight =>
            transaction.insert(flight).unit
          }
          .count
      }
      _ = insertedFlights should be(286463)
    } yield ()
  }

  case class Trip(path: TraversalPath[Flight, Airport, Airport]) {
    val totalMillis: Long = path.edges.last.arrival - path.edges.head.departure
    val totalHours: Long = totalMillis / (1000 * 60 * 60)
    val totalMinutes: Long = (totalMillis / (1000 * 60)) % 60
  }
}