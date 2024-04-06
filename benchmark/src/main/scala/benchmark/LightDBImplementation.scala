package benchmark

import cats.effect.IO
import cats.implicits.toTraverseOps
import fabric.rw.RW
import lightdb.{Document, Id, JsonMapping, LightDB}
import lightdb.collection.Collection
import lightdb.index.lucene.LuceneIndexerSupport
import lightdb.store.halo.{MultiHaloSupport, SharedHaloSupport}
import lightdb.index.lucene._
import lightdb.query.FieldQueryExtras

import java.nio.file.Paths

object LightDBImplementation extends BenchmarkImplementation {
  override type TitleAka = TitleAkaLDB
  override type TitleBasics = TitleBasicsLDB

  override def name: String = "LightDB"

  override def init(): IO[Unit] = db.init(truncate = true)

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

  override def persistTitleAka(t: TitleAkaLDB): IO[Unit] = db.titleAka.put(t).map(_ => ())

  override def persistTitleBasics(t: TitleBasicsLDB): IO[Unit] = db.titleBasics.put(t).map(_ => ())

  override def streamTitleAka(): fs2.Stream[IO, TitleAkaLDB] = db.titleAka.all()

  override def idFor(t: TitleAkaLDB): String = t._id.value

  override def titleIdFor(t: TitleAkaLDB): String = t.titleId

  override def get(id: String): IO[TitleAkaLDB] = db.titleAka.get(Id[TitleAkaLDB](id)).map(_.getOrElse(throw new RuntimeException(s"$id not found")))

  override def findByTitleId(titleId: String): IO[List[TitleAkaLDB]] = db.titleAka.query.filter(TitleAkaLDB.titleId === titleId).stream().compile.toList

  override def flush(): IO[Unit] = for {
    _ <- db.titleAka.commit()
    _ <- db.titleBasics.commit()
  } yield ()

  override def verifyTitleAka(): IO[Unit] = for {
    haloCount <- db.titleAka.store.count()
    luceneCount <- db.titleAka.indexer.count()
  } yield {
    scribe.info(s"TitleAka counts -- Halo: $haloCount, Lucene: $luceneCount")
  }

  override def verifyTitleBasics(): IO[Unit] = for {
    haloCount <- db.titleBasics.store.count()
    luceneCount <- db.titleBasics.indexer.count()
  } yield {
    scribe.info(s"TitleBasic counts -- Halo: $haloCount, Lucene: $luceneCount")
  }

  object db extends LightDB(directory = Some(Paths.get("imdb"))) with LuceneIndexerSupport with MultiHaloSupport {
    override protected def haloIndexThreads: Int = 10
    override protected def haloMaxFileSize: Int = 1024 * 1024 * 100    // 100 meg

    val titleAka: Collection[TitleAkaLDB] = collection("titleAka", TitleAkaLDB)
    val titleBasics: Collection[TitleBasicsLDB] = collection("titleBasics", TitleBasicsLDB)
  }

  case class TitleAkaLDB(titleId: String, ordering: Int, title: String, region: Option[String], language: Option[String], types: List[String], attributes: List[String], isOriginalTitle: Option[Boolean], _id: Id[TitleAka]) extends Document[TitleAka]

  object TitleAkaLDB extends JsonMapping[TitleAkaLDB] {
    override implicit val rw: RW[TitleAkaLDB] = RW.gen

    val titleId: FD[String] = field("titleId", _.titleId)
    val ordering: FD[Int] = field("ordering", _.ordering)
    val title: FD[String] = field("title", _.title)
  }

  case class TitleBasicsLDB(tconst: String, titleType: String, primaryTitle: String, originalTitle: String, isAdult: Boolean, startYear: Int, endYear: Int, runtimeMinutes: Int, genres: List[String], _id: Id[TitleBasics]) extends Document[TitleBasics]

  object TitleBasicsLDB extends JsonMapping[TitleBasicsLDB] {
    override implicit val rw: RW[TitleBasicsLDB] = RW.gen

    val tconst: FD[String] = field("tconst", _.tconst)
    val primaryTitle: FD[String] = field("primaryTitle", _.primaryTitle)
    val originalTitle: FD[String] = field("originalTitle", _.originalTitle)
  }
}
