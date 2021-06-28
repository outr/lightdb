package benchmark

import cats.effect.{ExitCode, IO, IOApp, Resource, SyncIO}
import fabric.rw._
import lightdb.field.Field
import lightdb.{Document, Id, JsonMapping, LightDB}
import lightdb.index.lucene._
import lightdb.store.halo.SharedHaloSupport

import java.io.{File, FileInputStream}
import java.nio.file.Paths
import java.util.zip.GZIPInputStream
import scala.io.Source
import fs2._
import fs2.compression._
import fs2.io.file._
import lightdb.collection.Collection
import perfolation._

// 64 seconds for zero processing
// 99 seconds for TitleAka parsing - Total: 26472316 in 98.456 seconds (268874.6 per second)
// 4.5 hours for complete - Total: 26472316 in 15966.323 seconds (1658.0 per second)
object IMDBBenchmark extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val baseDirectory = new File("../data/test/imdb")
    val start = System.currentTimeMillis()
    for {
      titleBasics <- processTSV(new File(baseDirectory, "title.akas.tsv.gz")).evalTap { map =>
        if (map.nonEmpty) {
          val t = TitleAka(
            titleId = map.value("titleId"),
            ordering = map.int("ordering"),
            title = map.value("title"),
            region = map.option("region"),
            language = map.option("language"),
            types = map.list("types"),
            attributes = map.list("attributes"),
            isOriginalTitle = map.boolOption("isOriginalTitle")
          )
          db.titleAka.put(t)
        } else {
          IO.unit
        }
      }.foldMap(_ => 1L).compile.lastOrError
    } yield {
//      scribe.info(s"First: ${titleBasics.next()}")
      val elapsed = (System.currentTimeMillis() - start) / 1000.0
      val perSecond = titleBasics / elapsed
      scribe.info(s"Total: $titleBasics in $elapsed seconds (${perSecond.f(f = 1)} per second)")
      ExitCode.Success
    }
  }

  implicit class MapExtras(map: Map[String, String]) {
    def option(key: String): Option[String] = map.get(key) match {
      case Some("") | None => None
      case Some(s) => Some(s)
    }
    def value(key: String): String = option(key).getOrElse(throw new RuntimeException(s"Key not found: $key in ${map.keySet}"))
    def int(key: String): Int = value(key).toInt
    def list(key: String): List[String] = option(key).map(_.split(' ').toList).getOrElse(Nil)
    def bool(key: String): Boolean = if (int(key) == 0) false else true
    def boolOption(key: String): Option[Boolean] = option(key).map(s => if (s.toInt == 0) false else true)
  }

  private def processTSV(file: File): Stream[IO, Map[String, String]] = {
    io.readInputStream[IO](IO(new GZIPInputStream(new FileInputStream(file))), 4096)
//    Files[IO].readAll(file.toPath, 4096)
//      .through(Compression[IO].inflate(InflateParams(bufferSize = 4096, header = ZLibParams.Header.GZIP)))
      .through(text.utf8Decode)
      .through(text.lines)
      .pull.uncons1
      .flatMap {
        case None => Pull.done
        case Some((header, rows)) => rows.map(header.split('\t').toList -> _).pull.echo
      }
      .stream
      .map {
        case (headers, row) => headers.zip(row.split('\t').toList).map(t => t._1 -> t._2.trim).filter(t => t._2.nonEmpty && t._2 != "\\N").toMap
      }
  }

  object db extends LightDB(directory = Some(Paths.get("imdb"))) with LuceneIndexerSupport with SharedHaloSupport {
    val titleAka: Collection[TitleAka] = collection[TitleAka]("titleAka", TitleAka)
  }

  case class TitleAka(titleId: String, ordering: Int, title: String, region: Option[String], language: Option[String], types: List[String], attributes: List[String], isOriginalTitle: Option[Boolean], _id: Id[TitleAka] = Id[TitleAka]()) extends Document[TitleAka]

  object TitleAka extends JsonMapping[TitleAka] {
    override implicit val rw: ReaderWriter[TitleAka] = ccRW

    val titleId: FD[String] = field("titleId", _.titleId).indexed()
    val ordering: FD[Int] = field("ordering", _.ordering).indexed()
    val title: FD[String] = field("title", _.title).indexed()
  }
}