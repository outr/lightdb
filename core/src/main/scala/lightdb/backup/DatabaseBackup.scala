package lightdb.backup

import cats.effect.IO
import cats.implicits.toTraverseOps
import fabric.io.JsonFormatter
import lightdb.collection.Collection
import lightdb.{KeyValue, LightDB}

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.zip.{ZipEntry, ZipOutputStream}

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
    db
      .collections
      .map(_.asInstanceOf[Collection[KeyValue, KeyValue.type]])
      .map { collection =>
        for {
          fileName <- IO(this.fileName(collection))
          entry <- IO(new ZipEntry(s"backup/$fileName"))
          _ <- IO(out.putNextEntry(entry))
          count <- collection.transaction { implicit transaction =>
            collection
              .jsonStream
              .map(JsonFormatter.Compact.apply)
              .map { line =>
                val bytes = s"$line\n".getBytes("UTF-8")
                out.write(bytes)
              }
              .compile
              .count
              .map(_.toInt)
              .flatTap(_ => IO(out.closeEntry()))
          }
        } yield count
      }
      .sequence
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
    db
      .collections
      .map(_.asInstanceOf[Collection[KeyValue, KeyValue.type]])
      .map { collection =>
        val fileName = this.fileName(collection)
        val file = new File(directory, fileName)
        val writer = new PrintWriter(file)
        collection.transaction { implicit transaction =>
          collection
            .jsonStream
            .map(JsonFormatter.Compact.apply)
            .map(writer.println)
            .compile
            .count
            .map(_.toInt)
        }
      }
      .sequence
      .map(_.sum)
  }

  private def fileName(collection: Collection[_, _]): String = s"${collection.name}.jsonl"
}