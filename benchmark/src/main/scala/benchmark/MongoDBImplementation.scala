package benchmark

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.mongodb.client.MongoClients
import lightdb.Unique
import lightdb.util.FlushingBacklog
import org.bson.Document

import java.{lang, util}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

object MongoDBImplementation extends BenchmarkImplementation {
  implicit val runtime: IORuntime = IORuntime.global

  override type TitleAka = Document

  private lazy val client = MongoClients.create()
  private lazy val db = client.getDatabase("imdb")
  private lazy val collection = db.getCollection("titleAka")

  override def name: String = "MongoDB"

  override def map2TitleAka(map: Map[String, String]): Document = {
    new Document(Map[String, AnyRef](
      "_id" -> Unique(),
      "titleId" -> map.value("titleId"),
      "ordering" -> Integer.valueOf(map.int("ordering")),
      "title" -> map.value("title"),
      "region" -> map.option("region").orNull,
      "language" -> map.option("language").orNull,
      "types" -> map.list("types").mkString(", "),
      "attributes" -> map.list("attributes").mkString(", "),
      "isOriginalTitle" -> map.boolOption("isOriginalTitle").map(lang.Boolean.valueOf).orNull
    ).asJava)
  }

  private lazy val backlog = new FlushingBacklog[Document](1000) {
    override protected def write(list: List[Document]): IO[Unit] = IO {
      val javaList = new util.ArrayList[Document](batchSize)
      list.foreach(javaList.add)
      collection.insertMany(javaList)
      ()
    }
  }

  override def persistTitleAka(t: Document)(implicit ec: ExecutionContext): Future[Unit] = backlog.enqueue(t).unsafeToFuture().map(_ => ())

  override def flush()(implicit ec: ExecutionContext): Future[Unit] = backlog.flush().unsafeToFuture()

  override def verifyTitleAka()(implicit ec: ExecutionContext): Future[Unit] = Future {
    val docs = collection.countDocuments()
    scribe.info(s"TitleAka counts -- $docs")
  }
}