package lightdb.backup

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeByName, toTraverseOps}
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.Asable
import lightdb.{Document, KeyValue, LightDB}
import lightdb.model.AbstractCollection

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.io.Source

object DatabaseBackup {
  /**
   * Creates a ZIP file backup of the database. Use with DatabaseRestore.archive to restore the backup archive.
   *
   * @param db      the database to backup
   * @param archive the ZIP file to create
   * @return the number of records backed up
   */
  def archive(db: LightDB,
              archive: File = new File("backup.zip")): IO[Int] = {
    val out = new ZipOutputStream(new FileOutputStream(archive))
    collectionStreams(db)
      .evalMap {
        case (fileName, stream) =>
          val entry = new ZipEntry(s"backup/$fileName")
          out.putNextEntry(entry)
          stream
            .map { line =>
              val bytes = s"$line\n".getBytes("UTF-8")
              out.write(bytes)
            }
            .compile
            .count
            .map(_.toInt)
            .flatTap { _ =>
              IO(out.closeEntry())
            }
      }
      .compile
      .toList
      .map(_.sum)
      .guarantee(IO {
        out.flush()
        out.close()
      })
  }

  /**
   * Does a full backup of the supplied database to the directory specified
   */
  def apply(db: LightDB, directory: File): IO[Int] = {
    directory.mkdirs()
    collectionStreams(db)
      .evalMap {
        case (fileName, stream) =>
          val file = new File(directory, fileName)
          val writer = new PrintWriter(file)
          stream
            .map(writer.println)
            .compile
            .count
            .map(_.toInt)
            .guarantee(IO {
              writer.flush()
              writer.close()
            })
      }
      .compile
      .toList
      .map(_.sum)
  }

  private def collectionStreams(db: LightDB): fs2.Stream[IO, (String, fs2.Stream[IO, String])] = fs2.Stream(db.collections: _*)
    .map { c =>
      val collection = c.asInstanceOf[AbstractCollection[KeyValue]]
      s"${c.collectionName}.jsonl" -> backupCollectionStream(collection)
    }

  private def backupCollectionStream[D <: Document[D]](collection: AbstractCollection[D]): fs2.Stream[IO, String] =
    collection.jsonStream.map(JsonFormatter.Compact(_))
}