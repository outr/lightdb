package benchmark

import cats.effect.IO
import fabric.rw.RW
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Collection, Document, Id, IndexedLinks, LightDB, MaxLinks}

import java.nio.file.Paths

object LightDBImplementation extends BenchmarkImplementation {
  override type TitleAka = TitleAkaLDB
  override type TitleBasics = TitleBasicsLDB

  override def name: String = "LightDB"

  override def init(): IO[Unit] = DB.init(truncate = true)

  override def map2TitleAka(map: Map[String, String]): TitleAkaLDB = TitleAkaLDB(
    titleId = map.value("titleId"),
    ordering = map.int("ordering"),
    title = map.value("title").replace("\\N", "N"),
    region = map.option("region"),
    language = map.option("language"),
    types = map.list("types"),
    attributes = map.list("attributes"),
    isOriginalTitle = map.boolOption("isOriginalTitle"),
    _id = Id[TitleAkaLDB]()
  )

  override def map2TitleBasics(map: Map[String, String]): TitleBasicsLDB = TitleBasicsLDB(
    tconst = map.value("tconst"),
    titleType = map.value("titleType"),
    primaryTitle = map.value("primaryTitle"),
    originalTitle = map.value("originalTitle"),
    isAdult = map.bool("isAdult"),
    startYear = map.int("startYear"),
    endYear = map.int("endYear"),
    runtimeMinutes = map.int("runtimeMinutes"),
    genres = map.list("genres"),
    _id = Id[TitleBasicsLDB]()
  )

  override def persistTitleAka(t: TitleAkaLDB): IO[Unit] = TitleAkaLDB.set(t).map(_ => ())

  override def persistTitleBasics(t: TitleBasicsLDB): IO[Unit] = TitleBasicsLDB.set(t).map(_ => ())

  override def streamTitleAka(): fs2.Stream[IO, TitleAkaLDB] = TitleAkaLDB.stream

  override def idFor(t: TitleAkaLDB): String = t._id.value

  override def titleIdFor(t: TitleAkaLDB): String = t.titleId

  override def get(id: String): IO[TitleAkaLDB] = TitleAkaLDB(Id[TitleAkaLDB](id))

  override def findByTitleId(titleId: String): IO[List[TitleAkaLDB]] = TitleAkaLDB.withSearchContext { implicit context =>
    TitleAkaLDB
      .query
      .pageSize(100)
      .filter(TitleAkaLDB.titleId === titleId)
      .search()
      .flatMap { page =>
        page.docs
      }
  }
//    TitleAkaLDB.titleId.query(titleId).compile.toList

  override def flush(): IO[Unit] = for {
    _ <- TitleAkaLDB.commit()
    _ <- TitleBasicsLDB.commit()
  } yield ()

  override def verifyTitleAka(): IO[Unit] = for {
    haloCount <- TitleAkaLDB.size
//    luceneCount <- Title DB.titleAka.indexer.count()
  } yield {
    scribe.info(s"TitleAka counts -- Halo: $haloCount") //, Lucene: $luceneCount")
  }

  override def verifyTitleBasics(): IO[Unit] = for {
    haloCount <- TitleBasicsLDB.size
//    luceneCount <- DB.titleBasics.indexer.count()
  } yield {
    scribe.info(s"TitleBasic counts -- Halo: $haloCount") //, Lucene: $luceneCount")
  }

  object DB extends LightDB(directory = Paths.get("imdb"), indexThreads = 10, maxFileSize = 1024 * 1024 * 100) {
//    val titleAka: Collection[TitleAkaLDB] = collection("titleAka", TitleAkaLDB)
//    val titleBasics: Collection[TitleBasicsLDB] = collection("titleBasics", TitleBasicsLDB)

    override def collections: List[Collection[_]] = List(
      TitleAkaLDB, TitleBasicsLDB
    )

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class TitleAkaLDB(titleId: String, ordering: Int, title: String, region: Option[String], language: Option[String], types: List[String], attributes: List[String], isOriginalTitle: Option[Boolean], _id: Id[TitleAka]) extends Document[TitleAka]

  object TitleAkaLDB extends Collection[TitleAkaLDB]("titleAka", DB) {
    override implicit val rw: RW[TitleAkaLDB] = RW.gen

//    val titleId: IndexedLinks[String, TitleAkaLDB] = indexedLinks[String]("titleId", identity, _.titleId, MaxLinks.OverflowTrim(100))
    val titleId: StringField[TitleAkaLDB] = index("titleId").string(_.titleId)
  }

  case class TitleBasicsLDB(tconst: String, titleType: String, primaryTitle: String, originalTitle: String, isAdult: Boolean, startYear: Int, endYear: Int, runtimeMinutes: Int, genres: List[String], _id: Id[TitleBasics]) extends Document[TitleBasics]

  object TitleBasicsLDB extends Collection[TitleBasicsLDB]("titleBasics", DB) {
    override implicit val rw: RW[TitleBasicsLDB] = RW.gen

//    val tconst: FD[String] = field("tconst", _.tconst)
//    val primaryTitle: FD[String] = field("primaryTitle", _.primaryTitle)
//    val originalTitle: FD[String] = field("originalTitle", _.originalTitle)
  }
}
