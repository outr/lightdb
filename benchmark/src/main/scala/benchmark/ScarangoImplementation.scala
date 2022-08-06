package benchmark

import cats.effect.IO
import com.outr.arango.collection.DocumentCollection
import com.outr.arango.query._
import com.outr.arango.{Document, DocumentModel, Field, Graph, Id, Index}
import fabric.rw.{ReaderWriter, ccRW}
import lightdb.util.FlushingBacklog

// TODO: Update Scarango for latest versions of dependencies
object ScarangoImplementation extends BenchmarkImplementation {
  override type TitleAka = TitleAkaADB
  override type TitleBasics = TitleBasicsADB

  private lazy val backlogAka = new FlushingBacklog[TitleAkaADB](1000, 10000) {
    override protected def write(list: List[TitleAkaADB]): IO[Unit] = db.titleAka.batch.insert(list).map(_ => ())
  }

  private lazy val backlogBasics = new FlushingBacklog[TitleBasicsADB](1000, 10000) {
    override protected def write(list: List[TitleBasicsADB]): IO[Unit] =
      db.titleBasics.batch.insert(list).map(_ => ())
  }

  override def name: String = "Scarango"

  override def init(): IO[Unit] = db.init()

  override def map2TitleAka(map: Map[String, String]): TitleAkaADB = {
    val title = map.value("title")
    val attributes = map.list("attributes")
    TitleAkaADB(
      titleId = map.value("titleId"),
      ordering = map.int("ordering"),
      title = title,
      region = map.option("region"),
      language = map.option("language"),
      types = map.list("types"),
      attributes = attributes,
      isOriginalTitle = map.boolOption("isOriginalTitle")
    )
  }

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

  override def streamTitleAka(): fs2.Stream[IO, TitleAkaADB] = db.titleAka.query.stream

  override def verifyTitleAka(): IO[Unit] = db.titleAka
    .query(aql"FOR d IN titleAka COLLECT WITH COUNT INTO length RETURN length")
    .as[Int]
    .one
    .map { count =>
      scribe.info(s"TitleAka counts -- $count")
    }

  override def verifyTitleBasics(): IO[Unit] = db.titleAka
    .query(aql"FOR d IN titleBasics COLLECT WITH COUNT INTO length RETURN length")
    .as[Int]
    .one
    .map { count =>
      scribe.info(s"TitleBasics counts -- $count")
    }

  override def get(id: String): IO[TitleAkaADB] = db.titleAka(TitleAkaADB.id(id))

  override def findByTitleId(titleId: String): IO[List[TitleAkaADB]] = db.titleAka
    .query(aql"FOR d IN titleAka FILTER d.${TitleAkaADB.titleId} == $titleId RETURN d")
    .all

  object db extends Graph("imdb") {
    val titleAka: DocumentCollection[TitleAkaADB] = vertex[TitleAkaADB](TitleAkaADB)
    val titleBasics: DocumentCollection[TitleBasicsADB] = vertex[TitleBasicsADB](TitleBasicsADB)
  }

  case class TitleAkaADB(titleId: String, ordering: Int, title: String, region: Option[String], language: Option[String], types: List[String], attributes: List[String], isOriginalTitle: Option[Boolean], _id: Id[TitleAkaADB] = TitleAkaADB.id()) extends Document[TitleAkaADB]

  object TitleAkaADB extends DocumentModel[TitleAkaADB] {
    override implicit val rw: ReaderWriter[TitleAkaADB] = ccRW

    val titleId: Field[String] = field("titleId")
    val ordering: Field[Int] = field("ordering")
    val title: Field[String] = field("title")

    override def indexes: List[Index] = List(titleId.index.persistent())

    override val collectionName: String = "titleAka"
  }

  case class TitleBasicsADB(tconst: String, titleType: String, primaryTitle: String, originalTitle: String, isAdult: Boolean, startYear: Int, endYear: Int, runtimeMinutes: Int, genres: List[String], _id: Id[TitleBasicsADB] = TitleBasicsADB.id()) extends Document[TitleBasicsADB]

  object TitleBasicsADB extends DocumentModel[TitleBasicsADB] {
    override implicit val rw: ReaderWriter[TitleBasicsADB] = ccRW

    val tconst: Field[String] = field("tconst")
    val primaryTitle: Field[String] = field("primaryTitle")
    val originalTitle: Field[String] = field("originalTitle")

    override def indexes: List[Index] = Nil

    override val collectionName: String = "titleBasics"
  }
}