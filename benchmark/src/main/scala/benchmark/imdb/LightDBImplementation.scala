/*
package benchmark.imdb

import cats.effect.IO
import fabric.rw.{Asable, RW}
import lightdb.KeyValue.I
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel, JsonConversion}
import lightdb.halodb.HaloDBStore
import lightdb.id.Id
import lightdb.store.StoreManager
import lightdb.transaction.Transaction
import lightdb.upgrade.DatabaseUpgrade
import rapid.Task

import java.nio.file.{Path, Paths}

object LightDBImplementation extends BenchmarkImplementation {
  override type TitleAka = TitleAkaLDB
  override type TitleBasics = TitleBasicsLDB

  override def name: String = "LightDB"

  override def init(): Task[Unit] = DB.init

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

  override def persistTitleAka(t: TitleAkaLDB): Task[Unit] = Task {
    if (akaTransaction == null) akaTransaction = DB.titleAka.transaction.create()
    DB.titleAka.set(t)
  }

  override def persistTitleBasics(t: TitleBasicsLDB): Task[Unit] = Task {
    if (basicsTransaction == null) basicsTransaction = DB.titleBasics.transaction.create()
    DB.titleBasics.set(t)
  }

  override def streamTitleAka(): rapid.Stream[TitleAkaLDB] = fs2.Stream.fromBlockingIterator[IO](DB.titleAka.iterator, 100)

  override def idFor(t: TitleAkaLDB): String = t._id.value

  override def titleIdFor(t: TitleAkaLDB): String = t.titleId

  override def get(id: String): Task[TitleAkaLDB] = IO(DB.titleAka(Id[TitleAkaLDB](id)))

  override def findByTitleId(titleId: String): Task[List[TitleAkaLDB]] = Task {
    DB.titleAka
      .query
      .filter(_.titleId === titleId)
      .search
      .materialized(t => t.indexes)
      .list
      .map(_.json.as[TitleAkaLDB])
  }
  //  override def findByTitleId(titleId: String): Task[List[TitleAkaLDB]] = TitleAkaLDB.titleId.query(titleId).compile.toList

  override def flush(): Task[Unit] = Task {
    akaTransaction.commit()
    basicsTransaction.commit()
  }

  override def verifyTitleAka(): Task[Unit] = Task {
    val haloCount = DB.titleAka.count
    val luceneCount = DB.titleAka.indexer.count
    scribe.info(s"TitleAka counts -- Halo: $haloCount, Lucene: $luceneCount")
  }

  override def verifyTitleBasics(): Task[Unit] = Task {
    val haloCount = DB.titleBasics.count
    //    luceneCount <- DB.titleBasics.indexer.count()
    scribe.info(s"TitleBasic counts -- Halo: $haloCount") //, Lucene: $luceneCount")
  }

  //  object DB extends LightDB(directory = Paths.get("imdb"), maxFileSize = 1024 * 1024 * 1024) {
  object DB extends LightDB {
    override type SM = StoreManager
    override val storeManager: SM = HaloDBStore

    override protected def truncateOnInit: Boolean = true

    override def directory: Option[Path] = Some(Paths.get("imdb"))

    val titleAka: S[TitleAkaLDB, TitleAkaLDB.type] = store(TitleAkaLDB)
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
                         types: List[String] = Nil,
                         attributes: List[String] = Nil,
                         isOriginalTitle: Option[Boolean],
                         _id: Id[TitleAka]) extends Document[TitleAka]

  object TitleAkaLDB extends DocumentModel[TitleAkaLDB] with JsonConversion[TitleAkaLDB] {
    override implicit val rw: RW[TitleAkaLDB] = RW.gen

    val titleId: I[String] = field.index(_.titleId)
    val ordering: I[Int] = field.index(_.ordering)
    val title: I[String] = field.index(_.title)
    val region: I[Option[String]] = field.index(_.region)
    val language: I[Option[String]] = field.index(_.language)
    val isOriginalTitle: I[Option[Boolean]] = field.index(_.isOriginalTitle)
  }

  case class TitleBasicsLDB(tconst: String,
                            titleType: String,
                            primaryTitle: String,
                            originalTitle: String,
                            isAdult: Boolean,
                            startYear: Int,
                            endYear: Int,
                            runtimeMinutes: Int,
                            genres: List[String],
                            _id: Id[TitleBasics]) extends Document[TitleBasics]

  object TitleBasicsLDB extends DocumentModel[TitleBasicsLDB] with JsonConversion[TitleBasicsLDB] {
    override implicit val rw: RW[TitleBasicsLDB] = RW.gen

    val tconst: F[String] = field(_.tconst)
    val titleType: F[String] = field(_.titleType)
    val primaryTitle: F[String] = field(_.primaryTitle)
    val originalTitle: F[String] = field(_.originalTitle)
    val isAdult: F[Boolean] = field(_.isAdult)
    val startYear: F[Int] = field(_.startYear)
    val endYear: F[Int] = field(_.endYear)
    val runtimeMinutes: F[Int] = field(_.runtimeMinutes)
    val genres: F[List[String]] = field(_.genres)
  }
}
*/
