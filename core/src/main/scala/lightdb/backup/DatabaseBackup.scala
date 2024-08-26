package lightdb.backup

import fabric.Json
import fabric.io.JsonFormatter
import lightdb.collection.Collection
import lightdb.{KeyValue, LightDB}

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
              archive: File = new File("backup.zip")): Int = {
    archive.getParentFile.mkdirs()
    if (archive.exists()) archive.delete()
    val out = new ZipOutputStream(new FileOutputStream(archive))
    try {
      process(db) {
        case (collection, iterator) =>
          val fileName = s"${collection.name}.jsonl"
          val entry = new ZipEntry(s"backup/$fileName")
          out.putNextEntry(entry)
          try {
            iterator.map { json =>
              val line = JsonFormatter.Compact(json)
              val bytes = s"$line\n".getBytes("UTF-8")
              out.write(bytes)
            }.length
          } finally {
            out.closeEntry()
          }
      }
    } finally {
      out.flush()
      out.close()
    }
  }

  /**
   * Does a full backup of the supplied database to the directory specified
   */
  def apply(db: LightDB, directory: File): Int = {
    directory.mkdirs()
    process(db) {
      case (collection, iterator) =>
        val fileName = s"${collection.name}.jsonl"
        val file = new File(directory, fileName)
        val writer = new PrintWriter(file)
        try {
          iterator.map { json =>
            val jsonString = JsonFormatter.Compact(json)
            writer.println(jsonString)
          }.length
        } finally {
          writer.flush()
          writer.close()
        }
    }
  }

  private def process(db: LightDB)(f: (Collection[_, _], Iterator[Json]) => Int): Int = {
    db.collections.map { collection =>
      collection.t.json.iterator { iterator =>
        f(collection, iterator)
      }
    }.sum
  }
}