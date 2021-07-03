package benchmark

import cats.effect.unsafe.IORuntime
import fabric.rw.{ReaderWriter, ccRW}
import lighdb.storage.mapdb.SharedMapDBSupport
import lightdb.{Document, Id, JsonMapping, LightDB}
import lightdb.collection.Collection
import lightdb.index.{Indexer, NullIndexer}
import lightdb.index.lucene.LuceneIndexerSupport
import lightdb.store.halo.SharedHaloSupport
import lightdb.index.lucene._
import lightdb.store.NullStoreSupport

import java.nio.file.Paths
import scala.concurrent.{ExecutionContext, Future}

object LightDBImplementation extends BenchmarkImplementation {
  implicit val runtime: IORuntime = IORuntime.global

  type TitleAka = TitleAkaLDB

  override def name: String = "LightDB"

  override def map2TitleAka(map: Map[String, String]): TitleAkaLDB = TitleAkaLDB(
    titleId = map.value("titleId"),
    ordering = map.int("ordering"),
    title = map.value("title"),
    region = map.option("region"),
    language = map.option("language"),
    types = map.list("types"),
    attributes = map.list("attributes"),
    isOriginalTitle = map.boolOption("isOriginalTitle")
  )

  override def persistTitleAka(t: TitleAkaLDB)(implicit ec: ExecutionContext): Future[Unit] = db.titleAka.put(t).unsafeToFuture().map(_ => ())

  override def flush()(implicit ec: ExecutionContext): Future[Unit] = db.titleAka.commit().unsafeToFuture()

  override def verifyTitleAka()(implicit ec: ExecutionContext): Future[Unit] = for {
    haloCount <- db.titleAka.store.count().unsafeToFuture()
    luceneCount <- db.titleAka.indexer.count().unsafeToFuture()
  } yield {
    scribe.info(s"TitleAka counts -- Halo: $haloCount, Lucene: $luceneCount")
    ()
  }

  object db extends LightDB(directory = Some(Paths.get("imdb"))) with LuceneIndexerSupport with SharedHaloSupport {
    override protected def haloIndexThreads: Int = 10
    override protected def haloMaxFileSize: Int = 1024 * 1024 * 10    // 10 meg

    val titleAka: Collection[TitleAkaLDB] = collection[TitleAkaLDB]("titleAka", TitleAkaLDB)
  }

  case class TitleAkaLDB(titleId: String, ordering: Int, title: String, region: Option[String], language: Option[String], types: List[String], attributes: List[String], isOriginalTitle: Option[Boolean], _id: Id[TitleAka] = Id[TitleAka]()) extends Document[TitleAka]

  object TitleAkaLDB extends JsonMapping[TitleAkaLDB] {
    override implicit val rw: ReaderWriter[TitleAkaLDB] = ccRW

    val titleId: FD[String] = field("titleId", _.titleId).indexed()
    val ordering: FD[Int] = field("ordering", _.ordering).indexed()
    val title: FD[String] = field("title", _.title).indexed()
  }
}
