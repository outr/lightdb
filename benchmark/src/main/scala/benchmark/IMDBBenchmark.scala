package benchmark

import cats.effect.unsafe.IORuntime
import cats.effect.IO

import java.io.{BufferedOutputStream, BufferedReader, File, FileInputStream, FileOutputStream, FileReader, PrintWriter}
import java.util.zip.GZIPInputStream
import scala.io.Source
import fs2._
import fs2.io.file._
import perfolation._
import scribe.{Level, Logger}

import java.net.{URI, URL}
import scala.annotation.tailrec
import sys.process._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

// ArangoDB Sync - Total Akas: 999999 and Total Basics: 999999 in 94.616 seconds (21138.1 per second)
// ArangoDB Async - Total Akas: 999999 and Total Basics: 999999 in 71.579 seconds (27941.1 per second)
// Scarango - Total Akas: 999999 and Total Basics: 999999 in 198.089 seconds (10096.5 per second)
// Scarango - Total Akas: 999999 and Total Basics: 999999 in 158.873 seconds (12588.7 per second) - with rewrite
// MongoDB - Total Akas: 999999 and Total Basics: 999999 in 69.857 seconds (28629.9 per second)


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

/*
[info] 2022.08.06 08:43:21:265 io-compute-17 INFO benchmark.IMDBBenchmark.io:90
[info]     Scarango - Total Akas: 999999 and Total Basics: 999999 in 162.301 seconds (12322.8 per second)
[info]       akasFile: 0.011s, akasProcess: 32.025s, basicsFile: 0.002s, basicsProcess: 31.846
[info]       flushing: 0.054s, verifiedAka: 0.125s, verifiedBasics: 0.131s, cycle: 12.752s
[info]       validateIds: 14.551s, validateTitles: 69.982s

[info]     ArangoDB Async - Total Akas: 999999 and Total Basics: 999999 in 66.557 seconds (30049.4 per second)
[info]       akasFile: 0.008s, akasProcess: 6.429s, basicsFile: 0.001s, basicsProcess: 6.762
[info]       flushing: 0.012s, verifiedAka: 0.003s, verifiedBasics: 0.002s, cycle: 6.324s
[info]       validateIds: 13.934s, validateTitles: 32.540s
 */
object IMDBBenchmark { // extends IOApp {
  implicit val runtime: IORuntime = IORuntime.global
  val implementation: BenchmarkImplementation = SQLiteImplementation

  private var ids: List[Ids] = Nil

  type TitleAka = implementation.TitleAka

  implicit class ElapsedIO[Return](io: IO[Return]) {
    def elapsed: IO[Elapsed[Return]] = {
      val start = System.currentTimeMillis()
      io.map { r =>
        Elapsed(r, (System.currentTimeMillis() - start) / 1000.0)
      }
    }

    def elapsedValue: IO[Double] = elapsed.map(_.elapsed)
  }

  case class Elapsed[Return](value: Return, elapsed: Double)

  def main(args: Array[String]): Unit = {
    Logger("com.oath.halodb").withMinimumLevel(Level.Warn).replace()

    val start = System.currentTimeMillis()
    val baseDirectory = new File("data")
    val io = for {
      _ <- implementation.init()
      _ = scribe.info("--- Stage 1 ---")
      akasFile <- downloadFile(new File(baseDirectory, "title.akas.tsv"), Limit.Unlimited).elapsed
      _ = scribe.info("--- Stage 2 ---")
      totalAka <- process(akasFile.value, implementation.map2TitleAka, implementation.persistTitleAka).elapsed
      _ = scribe.info("--- Stage 3 ---")
      basicsFile <- downloadFile(new File(baseDirectory, "title.basics.tsv"), Limit.Unlimited).elapsed
      _ = scribe.info("--- Stage 4 ---")
      totalBasics <- process(basicsFile.value, implementation.map2TitleBasics, implementation.persistTitleBasics).elapsed
      _ = scribe.info("--- Stage 5 ---")
      flushingTime <- implementation.flush().elapsedValue
      _ = scribe.info("--- Stage 6 ---")
      verifiedTitleAkaTime <- implementation.verifyTitleAka().elapsedValue
      verifiedTitleBasicsTime <- implementation.verifyTitleBasics().elapsedValue
      _ = scribe.info("--- Stage 7 ---")
      cycleTime <- cycleThroughEntireCollection(10).elapsedValue
      _ = scribe.info("--- Stage 8 ---")
      validationTime <- validateIds(ids).elapsedValue
      _ = scribe.info("--- Stage 9 ---")
      validateTitleTime <- validateTitleIds(ids).elapsedValue
      _ = scribe.info("--- Stage 10 ---")
    } yield {
      val elapsed = (System.currentTimeMillis() - start) / 1000.0
      val perSecond = (totalAka.value + totalBasics.value) / elapsed
      scribe.info(
        s"""${implementation.name} - Total Akas: ${totalAka.value} and Total Basics: ${totalBasics.value} in $elapsed seconds (${perSecond.f(f = 1)} per second)
           |  akasFile: ${akasFile.elapsed.f(f = 3)}s, akasProcess: ${totalAka.elapsed.f(f = 3)}s, basicsFile: ${basicsFile.elapsed.f(f = 3)}s, basicsProcess: ${totalBasics.elapsed.f(f = 3)}
           |  flushing: ${flushingTime.f(f = 3)}s, verifiedAka: ${verifiedTitleAkaTime.f(f = 3)}s, verifiedBasics: ${verifiedTitleBasicsTime.f(f = 3)}s, cycle: ${cycleTime.f(f = 3)}s
           |  validateIds: ${validationTime.f(f = 3)}s, validateTitles: ${validateTitleTime.f(f = 3)}s""".stripMargin)
    }
    io.unsafeRunSync()
    sys.exit(0)
  }

  private def process[T](file: File, map2t: Map[String, String] => T, persist: T => IO[Unit]): IO[Int] = IO {
    val reader = new BufferedReader(new FileReader(file))
    val counter = new AtomicInteger(0)
    val concurrency = 32
    try {
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

      val iterator = new IOIterator[T] {
        override def next(): IO[Option[T]] = IO {
          nextLine()
        }.map(_.map { line =>
          val values = line.split('\t').toList
          val map = keys.zip(values).filter(t => t._2.nonEmpty && t._2 != "\\N").toMap
          map2t(map)
        })
      }

      var running = true

      val io = iterator.stream(concurrency) { t =>
        counter.incrementAndGet()
        persist(t)
      }

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

      io.unsafeRunSync()
      running = false
    } finally {
      scribe.info(s"Finished with counter: $counter")
      reader.close()
    }

    counter.get()
  }

  private val counter = new AtomicInteger(0)

  def cycleThroughEntireCollection(idEvery: Int): IO[Unit] = implementation.streamTitleAka().map { titleAka =>
    val v = counter.incrementAndGet()
    if (v % idEvery == 0) {
      ids = Ids(implementation.idFor(titleAka), implementation.titleIdFor(titleAka)) :: ids
    }
  }.compile.drain.map { _ =>
    scribe.info(s"Counter for entire collection: ${counter.get()}")
  }

  def validateIds(idsList: List[Ids]): IO[Unit] = if (idsList.isEmpty) {
    IO.unit
  } else {
    val ids = idsList.head
    implementation.get(ids.id).flatMap { titleAka =>
      assert(titleAka != null, s"${ids.id} / ${ids.titleId} is null in lookup")
      val titleId = implementation.titleIdFor(titleAka)
      assert(titleId == ids.titleId, s"TitleID: $titleId was not expected: ${ids.titleId} for ${ids.id}")
      validateIds(idsList.tail)
    }
  }

  def validateTitleIds(idsList: List[Ids]): IO[Unit] = if (idsList.isEmpty) {
    IO.unit
  } else {
    val ids = idsList.head
    implementation.findByTitleId(ids.titleId).flatMap { titleAkas =>
      val results = titleAkas.map(ta => implementation.titleIdFor(ta))
      if (titleAkas.isEmpty) {
        throw new RuntimeException("Empty!")
      } else if (!results.forall(id => id == ids.titleId)) {
        throw new RuntimeException(s"Not all titleIds match the query: $results")
      }
      validateTitleIds(idsList.tail)
    }
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

  private def downloadFile(file: File, limit: Limit): IO[File] = (if (file.exists()) {
    IO.pure(file)
  } else {
    IO {
      scribe.info(s"File doesn't exist, downloading ${file.getName}...")
      file.getParentFile.mkdirs()
      val fileName = s"${file.getName}.gz"
      val gz = new File(file.getParentFile, fileName)
      val url = new URI(s"https://datasets.imdbws.com/$fileName").toURL
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
  }).flatMap { file =>
    limit match {
      case Limit.Unlimited => IO.pure(file)
      case _ => IO {
        val source = Source.fromFile(file)
        try {
          val (pre, post) = file.getName.splitAt(file.getName.lastIndexOf("."))
          val limitFile = new File(file.getParentFile, s"$pre.${limit.name}$post")
          if (!limitFile.exists()) {
            scribe.info(s"Creating limit file: ${limitFile.getName}...")
            val writer = new PrintWriter(limitFile)
            try {
              source.getLines().take(limit.value).foreach { line =>
                writer.write(s"$line\n")
              }
            } finally {
              writer.flush()
              writer.close()
            }
          }
          limitFile
        } finally {
          source.close()
        }
      }
    }
  }

  private def processTSV(file: File): Stream[IO, Map[String, String]] = {
//    io.readInputStream[IO](IO(new GZIPInputStream(new FileInputStream(file))), 4096)
    Files[IO].readAll(Path.fromNioPath(file.toPath), 4096, Flags.Read)
//      .through(Compression[IO].inflate(InflateParams(bufferSize = 4096, header = ZLibParams.Header.GZIP)))
      .through(text.utf8.decode)
      .through(text.lines)
      .pull
      .uncons1
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

case class Ids(id: String, titleId: String)

sealed trait Limit {
  def name: String
  def value: Int
}

object Limit {
  case object OneThousand extends Limit {
    override def name: String = "1k"
    override def value: Int = 1_000
  }
  case object TenThousand extends Limit {
    override def name: String = "10k"
    override def value: Int = 10_000
  }
  case object OneHundredThousand extends Limit {
    override def name: String = "100k"
    override def value: Int = 100_000
  }
  case object OneMillion extends Limit {
    override def name: String = "1m"
    override def value: Int = 1_000_000
  }
  case object TenMillion extends Limit {
    override def name: String = "10m"
    override def value: Int = 10_000_000
  }
  case object Unlimited extends Limit {
    override def name: String = ""
    override def value: Int = -1
  }
}