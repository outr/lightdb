package benchmark

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Indexes
import lightdb.util.Unique
import org.bson.Document

import java.{lang, util}
import scala.jdk.CollectionConverters._

object MongoDBImplementation extends BenchmarkImplementation {
  implicit val runtime: IORuntime = IORuntime.global

  override type TitleAka = Document
  override type TitleBasics = Document

  private lazy val client = MongoClients.create()
  private lazy val db = client.getDatabase("imdb")
  private lazy val titleAka = db.getCollection("titleAka")
  private lazy val titleBasics = db.getCollection("titleBasics")

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

  override def map2TitleBasics(map: Map[String, String]): Document = {
    new Document(Map[String, AnyRef](
      "_id" -> Unique(),
      "tconst" -> map.value("tconst"),
      "titleType" -> map.value("titleType"),
      "primaryTitle" -> map.value("primaryTitle"),
      "originalTitle" -> map.value("originalTitle"),
      "isAdult" -> map.value("isAdult"),
      "startYear" -> map.value("startYear"),
      "endYear" -> map.value("endYear"),
      "runtimeMinutes" -> map.value("runtimeMinutes"),
      "genres" -> map.list("genres").mkString(", ")
    ).asJava)
  }

  private lazy val backlogAka = new FlushingBacklog[String, Document](1000, 10000) {
    override protected def write(list: List[Document]): IO[Unit] = IO.blocking {
      val javaList = new util.ArrayList[Document](batchSize)
      list.foreach(javaList.add)
      titleAka.insertMany(javaList)
      ()
    }
  }

  private lazy val backlogBasics = new FlushingBacklog[String, Document](1000, 10000) {
    override protected def write(list: List[Document]): IO[Unit] = IO.blocking {
      val javaList = new util.ArrayList[Document](batchSize)
      list.foreach(javaList.add)
      titleBasics.insertMany(javaList)
      ()
    }
  }

  override def init(): IO[Unit] = IO.blocking {
    titleAka.createIndex(Indexes.ascending("titleId"))
  }

  override def persistTitleAka(t: Document): IO[Unit] = backlogAka.enqueue(t.getString("_id"), t).map(_ => ())

  override def persistTitleBasics(t: Document): IO[Unit] = backlogBasics.enqueue(t.getString("_id"), t).map(_ => ())

  override def streamTitleAka(): fs2.Stream[IO, Document] = {
    val iterator: Iterator[Document] = titleAka.find().iterator().asScala
    fs2.Stream.fromBlockingIterator[IO](iterator, 512)
  }

  override def idFor(t: Document): String = t.getString("_id")

  override def titleIdFor(t: Document): String = t.getString("titleId")

  import com.mongodb.client.model.Filters

  override def get(id: String): IO[Document] = IO.blocking {
    titleAka.find(Filters.eq("_id", id)).first()
  }

  override def findByTitleId(titleId: String): IO[List[Document]] = IO.blocking {
    titleAka.find(Filters.eq("titleId", titleId)).iterator().asScala.toList
  }

  override def flush(): IO[Unit] = for {
    _ <- backlogAka.flush()
    _ <- backlogBasics.flush()
  } yield {
    ()
  }

  override def verifyTitleAka(): IO[Unit] = IO.blocking {
    val docs = titleAka.countDocuments()
    scribe.info(s"TitleAka counts -- $docs")
  }

  override def verifyTitleBasics(): IO[Unit] = IO.blocking {
    val docs = titleBasics.countDocuments()
    scribe.info(s"TitleBasics counts -- $docs")
  }
}