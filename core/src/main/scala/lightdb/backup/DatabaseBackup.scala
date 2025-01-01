package lightdb.backup

import fabric.Json
import fabric.io.JsonFormatter
import lightdb.LightDB
import lightdb.collection.Collection
import rapid._

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.zip.{ZipEntry, ZipOutputStream}

object DatabaseBackup {
  /**
   * Creates a ZIP file backup of the database. Use with DatabaseRestore.archive to restore the backup archive.
   *
   * @param db the database to back up
   * @param archive the ZIP file to create
   * @return the number of records backed up
   */
  def archive(db: LightDB,
              archive: File = new File("backup.zip")): Task[Int] = Task {
    Option(archive.getParentFile).foreach(_.mkdirs())
    if (archive.exists()) archive.delete()
    val out = new ZipOutputStream(new FileOutputStream(archive))
    process(db) {
      case (collection, stream) => Task {
        val fileName = s"${collection.name}.jsonl"
        val entry = new ZipEntry(s"backup/$fileName")
        out.putNextEntry(entry)
        stream.map { json =>
          val line = JsonFormatter.Compact(json)
          val bytes = s"$line\n".getBytes("UTF-8")
          out.write(bytes)
        }.count.guarantee(Task(out.closeEntry()))
      }.flatten
    }.guarantee(Task {
      out.flush()
      out.close()
    })
  }.flatten

  /**
   * Does a full backup of the supplied database to the directory specified
   */
  def apply(db: LightDB, directory: File): Task[Int] = {
    directory.mkdirs()
    process(db) {
      case (collection, stream) => Task {
        val fileName = s"${collection.name}.jsonl"
        val file = new File(directory, fileName)
        val writer = new PrintWriter(file)
        stream.evalMap { json =>
          Task {
            val jsonString = JsonFormatter.Compact(json)
            writer.println(jsonString)
          }
        }.count.guarantee(Task {
          writer.flush()
          writer.close()
        })
      }.flatten
    }
  }

  private def process(db: LightDB)(f: (Collection[_, _], rapid.Stream[Json]) => Task[Int]): Task[Int] = {
    db.collections.map { collection =>
      collection.t.json.stream { stream =>
        f(collection, stream)
      }
    }.tasks.map(_.sum)
  }
}