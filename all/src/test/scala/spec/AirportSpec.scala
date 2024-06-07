package spec

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fabric.rw.RW
import lightdb.halo.HaloDBSupport
import lightdb.lucene.LuceneSupport
import lightdb.{Document, Id, LightDB, StoredValue}
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
