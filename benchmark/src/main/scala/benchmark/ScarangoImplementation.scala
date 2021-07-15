package benchmark

import cats.effect.IO
import com.outr.arango.query.AQLInterpolator
import com.outr.arango.{Document, DocumentCollection, DocumentModel, Field, Graph, Id, Index, Pagination, Serialization}
import lightdb.util.FlushingBacklog

import java.util.concurrent.{Executor, Executors}
import scala.concurrent.{ExecutionContext, Future}

object ScarangoImplementation extends BenchmarkImplementation {
  override type TitleAka = TitleAkaADB
  override type TitleBasics = TitleBasicsADB

  private implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(32))

  private lazy val backlogAka = new FlushingBacklog[TitleAkaADB](1000, 10000) {
    override protected def write(list: List[TitleAkaADB]): IO[Unit] = IO.fromFuture(IO {
      db.titleAka.insert(list).map(_ => ())
    })
  }

  private lazy val backlogBasics = new FlushingBacklog[TitleBasicsADB](1000, 10000) {
    override protected def write(list: List[TitleBasicsADB]): IO[Unit] = IO.fromFuture(IO {
      db.titleBasics.insert(list).map(_ => ())
    })
  }

  override def name: String = "Scarango"

  override def map2TitleAka(map: Map[String, String]): TitleAkaADB = TitleAkaADB(
    titleId = map.value("titleId"),
    ordering = map.int("ordering"),
    title = map.value("title"),
    region = map.option("region"),
    language = map.option("language"),
    types = map.list("types"),
    attributes = map.list("attributes"),
    isOriginalTitle = map.boolOption("isOriginalTitle")
  )

  override def map2TitleBasics(map: Map[String, String]): TitleBasicsADB = TitleBasicsADB(
    tconst = map.value("tconst"),
    titleType = map.value("titleType"),
    primaryTitle = map.value("primaryTitle"),
    originalTitle = map.value("originalTitle"),
    isAdult = map.bool("isAdult"),
    startYear = map.int("startYear"),
    endYear = map.int("endYear"),
    runtimeMinutes = map.int("runtimeMinutes"),
    genres = map.list("genres")
  )

  override def persistTitleAka(t: TitleAkaADB): IO[Unit] = backlogAka.enqueue(t).map(_ => ())

  override def persistTitleBasics(t: TitleBasicsADB): IO[Unit] = backlogBasics.enqueue(t).map(_ => ())

  override def flush(): IO[Unit] = for {
    _ <- backlogAka.flush()
    _ <- backlogBasics.flush()
  } yield {
    ()
  }

  override def idFor(t: TitleAkaADB): String = t._id.value

  override def titleIdFor(t: TitleAkaADB): String = t.titleId

  override def streamTitleAka(): fs2.Stream[IO, TitleAkaADB] = {
    def recursivePagination(paginationFuture: => Future[Pagination[TitleAkaADB]]): fs2.Stream[IO, TitleAkaADB] = {
      fs2.Stream.eval(IO.fromFuture(IO(paginationFuture))).flatMap { page =>
        val results = fs2.Stream.emits(page.results)
        if (page.hasNext) {
          results ++ recursivePagination(page.next())
        } else {
          results
        }
      }
    }

    recursivePagination(db.titleAka.all.batchSize(1000).paged)
  }

  override def verifyTitleAka(): IO[Unit] = IO.fromFuture(IO(db.titleAka
    .query(aql"FOR d IN $TitleAkaADB COLLECT WITH COUNT INTO length RETURN length")
    .as[Int]
    .one)).map { count =>
      scribe.info(s"TitleAka counts -- $count")
    }

  override def verifyTitleBasics(): IO[Unit] = IO.fromFuture(IO(db.titleAka
    .query(aql"FOR d IN $TitleBasicsADB COLLECT WITH COUNT INTO length RETURN length")
    .as[Int]
    .one)).map { count =>
      scribe.info(s"TitleBasics counts -- $count")
    }

  override def get(id: String): IO[TitleAkaADB] = IO.fromFuture(IO(db.titleAka(TitleAkaADB.id(id))))

  override def findByTitleId(titleId: String): IO[List[TitleAkaADB]] = IO.fromFuture(IO(db.titleAka
    .query(aql"FOR d IN $TitleAkaADB FILTER ${TitleAkaADB.titleId} == $titleId RETURN d")
    .results))

  object db extends Graph("imdb") {
    val titleAka: DocumentCollection[TitleAkaADB] = vertex[TitleAkaADB]
    val titleBasics: DocumentCollection[TitleBasicsADB] = vertex[TitleBasicsADB]
  }

  case class TitleAkaADB(titleId: String, ordering: Int, title: String, region: Option[String], language: Option[String], types: List[String], attributes: List[String], isOriginalTitle: Option[Boolean], _id: Id[TitleAkaADB] = TitleAkaADB.id()) extends Document[TitleAkaADB]

  object TitleAkaADB extends DocumentModel[TitleAkaADB] {
    val titleId: Field[String] = field("titleId")
    val ordering: Field[Int] = field("ordering")
    val title: Field[String] = field("title")

    override def indexes: List[Index] = List(titleId.index.persistent())

    override val collectionName: String = "titleAka"
    override implicit val serialization: Serialization[TitleAkaADB] = Serialization.auto[TitleAkaADB]
  }

  case class TitleBasicsADB(tconst: String, titleType: String, primaryTitle: String, originalTitle: String, isAdult: Boolean, startYear: Int, endYear: Int, runtimeMinutes: Int, genres: List[String], _id: Id[TitleBasicsADB] = TitleBasicsADB.id()) extends Document[TitleBasicsADB]

  object TitleBasicsADB extends DocumentModel[TitleBasicsADB] {
    val tconst: Field[String] = field("tconst")
    val primaryTitle: Field[String] = field("primaryTitle")
    val originalTitle: Field[String] = field("originalTitle")

    override def indexes: List[Index] = Nil

    override val collectionName: String = "titleBasics"
    override implicit val serialization: Serialization[TitleBasicsADB] = Serialization.auto[TitleBasicsADB]
  }

  case class Airport(name: String,
                     city: String,
                     state: String,
                     country: String,
                     lat: Double,
                     long: Double,
                     vip: Boolean,
                     _id: Id[Airport] = Airport.id()) extends Document[Airport]

  object Airport extends DocumentModel[Airport] {
    val name: Field[String] = Field[String]("name")

    override def indexes: List[Index] = Nil

    override val collectionName: String = "airports"
    override implicit val serialization: Serialization[Airport] = Serialization.auto[Airport]
  }
}