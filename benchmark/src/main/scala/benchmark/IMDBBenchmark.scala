package benchmark

import cats.effect.unsafe.IORuntime
import cats.effect.IO

import java.io.{BufferedOutputStream, BufferedReader, File, FileInputStream, FileOutputStream, FileReader}
import java.util.zip.GZIPInputStream
import scala.io.Source
import fs2._
import fs2.io.file._
import perfolation._

import java.net.URL
import scala.annotation.tailrec
import sys.process._
import scribe.Execution.global

import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

// PostgreSQL - Total: 26838043 in 547.247 seconds (49041.9 per second)
// MongoDB - Total: 26838043 in 605.694 seconds (44309.6 per second)
// LightDB - Total: 26838043 in 280.04 seconds (95836.5 per second) - (HaloDB / NullIndexer)
// LightDB - Total: 26838043 in 9016.418 seconds (2976.6 per second) - (HaloDB / Lucene)

// LightDB    - Total: 999999 in 186.082 seconds (5374.0 per second) - 16 threads (HaloDB / Lucene)
// LightDB    - Total: 999999 in 261.31 seconds (3826.9 per second) - 16 threads (MapDB / Lucene)
// LightDB    - Total: 999999 in 183.937 seconds (5436.6 per second) - 16 threads (NullStore / Lucene)
// LightDB    - Total: 999999 in 11.715 seconds (85360.6 per second) - 16 threads (HaloDB / NullIndexer)

// MongoDB    - Total: 999999 in 19.165 seconds (52178.4 per second)
// PostgreSQL - Total: 999999 in 15.947 seconds (62707.7 per second)
object IMDBBenchmark { // extends IOApp {
  implicit val runtime: IORuntime = IORuntime.global
  val implementation: BenchmarkImplementation = LightDBImplementation

  type TitleAka = implementation.TitleAka

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val baseDirectory = new File("data")
    val future = for {
      _ <- implementation.init()
      file <- downloadFile(new File(baseDirectory, "title.akas.tsv")).unsafeToFuture()
      total <- process(file)
      _ <- implementation.flush()
      _ <- implementation.verifyTitleAka()
    } yield {
      val elapsed = (System.currentTimeMillis() - start) / 1000.0
      val perSecond = total / elapsed
      scribe.info(s"${implementation.name} - Total: $total in $elapsed seconds (${perSecond.f(f = 1)} per second)")
    }
    Await.result(future, Duration.Inf)
    sys.exit(0)
  }

  private def process(file: File): Future[Int] = Future {
    val reader = new BufferedReader(new FileReader(file))
    val counter = new AtomicInteger(0)
    val concurrency = 16
    try {
      val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(concurrency))
      val keys = reader.readLine().split('\t').toList

      val hasMoreLines = new AtomicBoolean(true)
      def nextLine(): Option[String] = {
        if (hasMoreLines.get()) {
          val o = Option(reader.readLine())
          if (o.isEmpty) {
            hasMoreLines.set(false)
          }
          o
        } else {
          None
        }
      }

      val iterator = new FutureIterator[TitleAka] {
        override def next()(implicit ec: ExecutionContext): Future[Option[TitleAka]] = Future {
          nextLine()
        }(ec).map(_.map { line =>
          val values = line.split('\t').toList
          val map = keys.zip(values).filter(t => t._2.nonEmpty && t._2 != "\\N").toMap
          implementation.map2TitleAka(map)
        })(ec)
      }

      var running = true

      val future = iterator.stream(concurrency) { t =>
        counter.incrementAndGet()
        val start = System.currentTimeMillis()
        implementation.persistTitleAka(t).map { _ =>
          val elapsed = System.currentTimeMillis() - start
//          if (elapsed > 1000) scribe.warn(s"Took way too long: $elapsed")
        }
      }(ec)

      new Thread() {
        private val startTime = System.currentTimeMillis()

        override def run(): Unit = {
          while (running) {
            Thread.sleep(10000L)
            val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
            val perSecond = counter.get() / elapsed
            scribe.info(s"Processed: $counter in $elapsed seconds (${perSecond.f(f = 1)} per second) - Active threads: ${iterator.running.get()}")
          }
        }
      }.start()

      Await.result(future, Duration.Inf)
      running = false
    } finally {
      scribe.info(s"Finished with counter: $counter")
      reader.close()
    }

    counter.get()
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

      scribe.info(s"Unzipping ${file.getName}...")
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
}