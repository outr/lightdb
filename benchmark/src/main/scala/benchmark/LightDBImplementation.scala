package benchmark

import cats.effect.IO
import fabric.rw.RW
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.halo.HaloDBStore
import lightdb.index.{Indexed, IndexedCollection}
import lightdb.lucene.LuceneIndexer
import lightdb.store.StoreManager
import lightdb.transaction.Transaction
import lightdb.upgrade.DatabaseUpgrade
import lightdb.{Id, LightDB}

import java.nio.file.{Path, Paths}

object LightDBImplementation extends BenchmarkImplementation {
  override type TitleAka = TitleAkaLDB
  override type TitleBasics = TitleBasicsLDB

  override def name: String = "LightDB"

  override def init(): IO[Unit] = DB.init().void

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

  private implicit var akaTransaction: Transaction[TitleAkaLDB] = _
  private implicit var basicsTransaction: Transaction[TitleBasicsLDB] = _

  override def persistTitleAka(t: TitleAkaLDB): IO[Unit] = {
    if (akaTransaction == null) akaTransaction = DB.titleAka.transaction.create().unsafeRunSync()(cats.effect.unsafe.implicits.global)
    DB.titleAka.set(t).void
  }

  override def persistTitleBasics(t: TitleBasicsLDB): IO[Unit] = {
    if (basicsTransaction == null) basicsTransaction = DB.titleBasics.transaction.create().unsafeRunSync()(cats.effect.unsafe.implicits.global)
    DB.titleBasics.set(t).void
  }

  override def streamTitleAka(): fs2.Stream[IO, TitleAkaLDB] = DB.titleAka.stream

  override def idFor(t: TitleAkaLDB): String = t._id.value

  override def titleIdFor(t: TitleAkaLDB): String = t.titleId

  override def get(id: String): IO[TitleAkaLDB] = DB.titleAka(Id[TitleAkaLDB](id))

  override def findByTitleId(titleId: String): IO[List[TitleAkaLDB]] =
    DB.titleAka
      .query
      .limit(100)
      .filter(_.titleId === titleId)
      .stream
      .docs
      .compile
      .toList
  //  override def findByTitleId(titleId: String): IO[List[TitleAkaLDB]] = TitleAkaLDB.titleId.query(titleId).compile.toList

  override def flush(): IO[Unit] = for {
    _ <- akaTransaction.commit()
    _ <- basicsTransaction.commit()
  } yield ()

  override def verifyTitleAka(): IO[Unit] = for {
    haloCount <- DB.titleAka.count
    luceneCount <- DB.titleAka.indexer.count
  } yield {
    scribe.info(s"TitleAka counts -- Halo: $haloCount, Lucene: $luceneCount")
  }

  override def verifyTitleBasics(): IO[Unit] = for {
    haloCount <- DB.titleBasics.count
    //    luceneCount <- DB.titleBasics.indexer.count()
  } yield {
    scribe.info(s"TitleBasic counts -- Halo: $haloCount") //, Lucene: $luceneCount")
  }

  //  object DB extends LightDB(directory = Paths.get("imdb"), maxFileSize = 1024 * 1024 * 1024) {
  object DB extends LightDB {
    override protected def truncateOnInit: Boolean = true

    override def directory: Path = Paths.get("imdb")

    override def storeManager: StoreManager = HaloDBStore

    val titleAka: IndexedCollection[TitleAkaLDB, TitleAkaLDB.type] = collection("titleAka", TitleAkaLDB, LuceneIndexer(TitleAkaLDB))
    val titleBasics: Collection[TitleBasicsLDB, TitleBasicsLDB.type] = collection("titleBasics", TitleBasicsLDB)

    //    val titleAka: Collection[TitleAkaLDB] = collection("titleAka", TitleAkaLDB)
    //    val titleBasics: Collection[TitleBasicsLDB] = collection("titleBasics", TitleBasicsLDB)

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

  object TitleAkaLDB extends DocumentModel[TitleAkaLDB] with Indexed[TitleAkaLDB] {
    override implicit val rw: RW[TitleAkaLDB] = RW.gen

    val titleId: I[String] = index.one("titleId", _.titleId)
  }

  case class TitleBasicsLDB(tconst: String, titleType: String, primaryTitle: String, originalTitle: String, isAdult: Boolean, startYear: Int, endYear: Int, runtimeMinutes: Int, genres: List[String], _id: Id[TitleBasics]) extends Document[TitleBasics]

  object TitleBasicsLDB extends DocumentModel[TitleBasicsLDB] {
    override implicit val rw: RW[TitleBasicsLDB] = RW.gen

    //    val tconst: FD[String] = field("tconst", _.tconst)
    //    val primaryTitle: FD[String] = field("primaryTitle", _.primaryTitle)
    //    val originalTitle: FD[String] = field("originalTitle", _.originalTitle)
  }
}
