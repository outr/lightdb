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

import java.net.URL
import scala.annotation.tailrec
import sys.process._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

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
  val implementation: BenchmarkImplementation = ArangoDBImplementation

  private var ids: List[Ids] = Nil

  type TitleAka = implementation.TitleAka

  def main(args: Array[String]): Unit = {
    Logger("com.oath.halodb").withMinimumLevel(Level.Warn).replace()

    val start = System.currentTimeMillis()
    val baseDirectory = new File("data")
    var now = System.currentTimeMillis()
    val io = for {
      _ <- implementation.init()
      _ = scribe.info("--- Stage 1 ---")
      akasFile <- downloadFile(new File(baseDirectory, "title.akas.tsv"), Limit.OneMillion)
      _ = scribe.info("--- Stage 2 ---")
      totalAka <- process(akasFile, implementation.map2TitleAka, implementation.persistTitleAka)
      _ = scribe.info("--- Stage 3 ---")
      basicsFile <- downloadFile(new File(baseDirectory, "title.basics.tsv"), Limit.OneMillion)
      _ = scribe.info("--- Stage 4 ---")
      totalBasics <- process(basicsFile, implementation.map2TitleBasics, implementation.persistTitleBasics)
      _ = scribe.info("--- Stage 5 ---")
      _ <- implementation.flush()
      _ = scribe.info("--- Stage 6 ---")
      _ <- implementation.verifyTitleAka()
      _ <- implementation.verifyTitleBasics()
      _ = scribe.info("--- Stage 7 ---")
      _ = now = System.currentTimeMillis()
      _ <- cycleThroughEntireCollection(10)
      _ = scribe.info("--- Stage 8 ---")
      _ = scribe.info(s"Processed entire collection in ${(System.currentTimeMillis() - now) / 1000.0} seconds. Id list of ${ids.length}")
      _ = now = System.currentTimeMillis()
      _ <- validateIds(ids)
      _ = scribe.info("--- Stage 9 ---")
      _ = scribe.info(s"Validated ${ids.length} in ${(System.currentTimeMillis() - now) / 1000.0} seconds.")
      _ = now = System.currentTimeMillis()
      _ <- validateTitleIds(ids)
      _ = scribe.info("--- Stage 10 ---")
      _ = scribe.info(s"Validated ${ids.length} titleIds in ${(System.currentTimeMillis() - now) / 1000.0} seconds.")
    } yield {
      val elapsed = (System.currentTimeMillis() - start) / 1000.0
      val perSecond = (totalAka + totalBasics) / elapsed
      scribe.info(s"${implementation.name} - Total Akas: $totalAka and Total Basics: $totalBasics in $elapsed seconds (${perSecond.f(f = 1)} per second)")
    }
    io.unsafeRunSync()
    sys.exit(0)
  }

  private def process[T](file: File, map2t: Map[String, String] => T, persist: T => IO[Unit]): IO[Int] = IO {
    val reader = new BufferedReader(new FileReader(file))
    val counter = new AtomicInteger(0)
    val concurrency = 16
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
        val start = System.currentTimeMillis()
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
      titleAkas.find(ta => implementation.idFor(ta) == ids.id).getOrElse(throw new RuntimeException(s"Unable to find id match (${ids.id}) for: $titleAkas"))
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