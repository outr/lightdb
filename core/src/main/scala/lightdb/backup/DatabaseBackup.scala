package lightdb.backup

import fabric.Json
import fabric.io.JsonFormatter
import lightdb.store.Store
import rapid._

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.zip.{ZipEntry, ZipOutputStream}

object DatabaseBackup {
  /**
   * Creates a ZIP file backup of the database. Use with DatabaseRestore.archive to restore the backup archive.
   *
   * @param stores the stores to back up
   * @param archive the ZIP file to create
   * @return the number of records backed up
   */
  def archive(stores: List[Store[_, _]],
              archive: File = new File("backup.zip")): Task[Int] = Task {
    Option(archive.getParentFile).foreach(_.mkdirs())
    if (archive.exists()) archive.delete()
    val out = new ZipOutputStream(new FileOutputStream(archive))
    process(stores) {
      case (store, stream) => Task {
        val fileName = s"${store.name}.jsonl"
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
  def apply(stores: List[Store[_, _]], directory: File): Task[Int] = {
    directory.mkdirs()
    process(stores) {
      case (store, stream) => Task {
        val fileName = s"${store.name}.jsonl"
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

  private def process(stores: List[Store[_, _]])(f: (Store[_, _], rapid.Stream[Json]) => Task[Int]): Task[Int] = {
    stores.map { store =>
      store.t.json.stream { stream =>
        f(store, stream)
      }
    }.tasks.map(_.sum)
  }
}