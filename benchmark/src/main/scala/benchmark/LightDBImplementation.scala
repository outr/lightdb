package benchmark

import cats.effect.IO
import fabric.rw.RW
import lightdb.halo.HaloDBSupport
import lightdb.lucene.{LuceneIndex, LuceneSupport}
import lightdb.model.Collection
import lightdb.sqlite.{SQLData, SQLIndexedField, SQLiteSupport}
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Document, Id, IndexedLinks, LightDB, MaxLinks}

import java.nio.file.{Path, Paths}
import java.sql.ResultSet

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
//  override def findByTitleId(titleId: String): IO[List[TitleAkaLDB]] = TitleAkaLDB.titleId.query(titleId).compile.toList

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

//  object DB extends LightDB(directory = Paths.get("imdb"), maxFileSize = 1024 * 1024 * 1024) {
  object DB extends LightDB with HaloDBSupport {
  override def directory: Path = Paths.get("imdb")

  override def maxFileSize: Int = 1024 * 1024 * 1024

  //    val titleAka: Collection[TitleAkaLDB] = collection("titleAka", TitleAkaLDB)
//    val titleBasics: Collection[TitleBasicsLDB] = collection("titleBasics", TitleBasicsLDB)

    override def collections: List[Collection[_]] = List(
      TitleAkaLDB, TitleBasicsLDB
    )

    override def upgrades: List[DatabaseUpgrade] = Nil
  }

  case class TitleAkaLDB(titleId: String,
                         ordering: Int,
                         title: String,
                         region: Option[String],
                         language: Option[String],
                         types: List[String],
                         attributes: List[String],
                         isOriginalTitle: Option[Boolean],
                         _id: Id[TitleAka]) extends Document[TitleAka]

  object TitleAkaLDB extends Collection[TitleAkaLDB]("titleAka", DB) with SQLiteSupport[TitleAkaLDB] {
//  object TitleAkaLDB extends Collection[TitleAkaLDB]("titleAka", DB) with LuceneSupport[TitleAkaLDB] {
    override implicit val rw: RW[TitleAkaLDB] = RW.gen

//    val titleId: IndexedLinks[String, TitleAkaLDB] = indexedLinks[String]("titleId", identity, _.titleId, MaxLinks.OverflowTrim(100))
//    val titleId: LuceneIndex[String, TitleAkaLDB] = index.one("titleId", _.titleId)
    val titleId: SQLIndexedField[String, TitleAkaLDB] = index("titleId", doc => Some(doc.titleId))
//    val ordering: SQLIndexedField[Int, TitleAkaLDB] = index("ordering", doc => Some(doc.ordering))
//    val title: SQLIndexedField[String, TitleAkaLDB] = index("title", doc => Some(doc.title))
//    val region: SQLIndexedField[String, TitleAkaLDB] = index("region", _.region)
//    val language: SQLIndexedField[String, TitleAkaLDB] = index("language", _.language)
//    val types: SQLIndexedField[String, TitleAkaLDB] = index("types", doc => Some(doc.types.mkString("|")))
//    val attributes: SQLIndexedField[String, TitleAkaLDB] = index("attributes", doc => Some(doc.attributes.mkString("|")))
//    val isOriginalTitle: SQLIndexedField[Boolean, TitleAkaLDB] = index("isOriginalTitle", doc => doc.isOriginalTitle)

    /*override protected def data(rs: ResultSet): SQLData[TitleAkaLDB] = {
      val iterator = new Iterator[TitleAkaLDB] {
        override def hasNext: Boolean = rs.next()
        override def next(): TitleAkaLDB = TitleAkaLDB(
          titleId = rs.getString("titleId"),
          ordering = rs.getInt("ordering"),
          title = rs.getString("title"),
          region = None, //Option(rs.getString("region")),
          language = None, //Option(rs.getString("language")),
          types = Nil, //rs.getString("types").split('|').toList,
          attributes = Nil, //rs.getString("attributes").split('|').toList,
          isOriginalTitle = None,
          _id = Id[TitleAkaLDB](rs.getString("_id"))
        )
      }
      val list = iterator.toList
      val ids = list.map(_._id)
      val map = list.map(t => t._id -> t).toMap
      SQLData(ids, Some(id => IO(map(id))))
    }*/
  }

  case class TitleBasicsLDB(tconst: String, titleType: String, primaryTitle: String, originalTitle: String, isAdult: Boolean, startYear: Int, endYear: Int, runtimeMinutes: Int, genres: List[String], _id: Id[TitleBasics]) extends Document[TitleBasics]

  object TitleBasicsLDB extends Collection[TitleBasicsLDB]("titleBasics", DB) {
    override implicit val rw: RW[TitleBasicsLDB] = RW.gen

//    val tconst: FD[String] = field("tconst", _.tconst)
//    val primaryTitle: FD[String] = field("primaryTitle", _.primaryTitle)
//    val originalTitle: FD[String] = field("originalTitle", _.originalTitle)
  }
}
