package benchmark

import cats.effect.unsafe.IORuntime
import cats.effect.{ExitCode, IO, IOApp, Resource, SyncIO}
import fabric.rw._
import lightdb.field.Field
import lightdb.{Document, Id, JsonMapping, LightDB}
import lightdb.index.lucene._
import lightdb.store.halo.SharedHaloSupport

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.nio.file.Paths
import java.util.zip.GZIPInputStream
import scala.io.Source
import fs2._
import fs2.compression._
import fs2.io.file._
import lightdb.collection.Collection
import perfolation._

import java.net.URL
import scala.annotation.tailrec
import sys.process._
import scribe.Execution.global

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

// 64 seconds for zero processing
// 99 seconds for TitleAka parsing - Total: 26838044 in 95.132 seconds (282113.7 per second)
// What the crap - Old-school: 54 seconds for TitleAka parsing - Total: 26838043 in 53.17 seconds (504759.1 per second)
// 4.5 hours for complete - Total: 26472316 in 15966.323 seconds (1658.0 per second)
object IMDBBenchmark { // extends IOApp {
  implicit val runtime: IORuntime = IORuntime.global

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val baseDirectory = new File("data")
    val future = for {
      file <- downloadFile(new File(baseDirectory, "title.akas.tsv")).unsafeToFuture()
      total <- process(file)
    } yield {
      val elapsed = (System.currentTimeMillis() - start) / 1000.0
      val perSecond = total / elapsed
      scribe.info(s"Total: $total in $elapsed seconds (${perSecond.f(f = 1)} per second)")
    }
    Await.result(future, Duration.Inf)
  }

  private def process(file: File): Future[Int] = Future {
    val source = Source.fromFile(file)
    var count = 0
    try {
      var keys = List.empty[String]
      source.getLines().foreach { line =>
        val values = line.split('\t').toList
        if (keys.isEmpty) {
          keys = values
        } else {
          val map = keys.zip(values).filter(t => t._2.nonEmpty && t._2 != "\\N").toMap
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

//            db.titleAka.put(t)
            count += 1
          }
        }
      }
    } finally {
      source.close()
    }
    count
  }

//  override def run(args: List[String]): IO[ExitCode] = {
//    val baseDirectory = new File("data")
//    val start = System.currentTimeMillis()
//    for {
//      titleBasics <- downloadFile(new File(baseDirectory, "title.akas.tsv")).flatMap { file =>
//        processTSV(file).evalTap { map =>
//          if (map.nonEmpty) {
//            val t = TitleAka(
//              titleId = map.value("titleId"),
//              ordering = map.int("ordering"),
//              title = map.value("title"),
//              region = map.option("region"),
//              language = map.option("language"),
//              types = map.list("types"),
//              attributes = map.list("attributes"),
//              isOriginalTitle = map.boolOption("isOriginalTitle")
//            )
////            db.titleAka.put(t)
//            IO.unit
//          } else {
//            IO.unit
//          }
//        }.foldMap(_ => 1L).compile.lastOrError
//      }
//    } yield {
////      scribe.info(s"First: ${titleBasics.next()}")
//      val elapsed = (System.currentTimeMillis() - start) / 1000.0
//      val perSecond = titleBasics / elapsed
//      scribe.info(s"Total: $titleBasics in $elapsed seconds (${perSecond.f(f = 1)} per second)")
//      ExitCode.Success
//    }
//  }

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

  private def downloadFile(file: File): IO[File] = if (file.exists()) {
    IO.pure(file)
  } else {
    IO {
      scribe.info(s"File doesn't exist, downloading ${file.getName}...")
      file.getParentFile.mkdirs()
      val fileName = s"${file.getName}.gz"
      val gz = new File(file.getParentFile, fileName)
      val url = new URL(s"https://datasets.imdbws.com/$fileName")
      (url #> gz).!!

      scribe.info(s"sUnzipping ${file.getName}...")
      val input = new GZIPInputStream(new FileInputStream(gz))
      val output = new BufferedOutputStream(new FileOutputStream(file))

      val buf = new Array[Byte](1024)

      @tailrec
      def write(): Unit = input.read(buf) match {
        case n if n < 0 => // Finished
        case n =>
          output.write(buf, 0, n)
          write()
      }

      try {
        write()
      } finally {
        output.flush()
        output.close()
        input.close()
      }
      if (gz.delete()) {
        gz.deleteOnExit()
      }
      scribe.info(s"${file.getName} downloaded and extracted successfully")
      file
    }
  }

  private def processTSV(file: File): Stream[IO, Map[String, String]] = {
//    io.readInputStream[IO](IO(new GZIPInputStream(new FileInputStream(file))), 4096)
    Files[IO].readAll(file.toPath, 4096)
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