package lightdb.backup

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
              archive: File = new File("backup.zip")): Int = {
    val out = new ZipOutputStream(new FileOutputStream(archive))
    try {
      db
        .collections
        .map(_.asInstanceOf[Collection[KeyValue, KeyValue.type]])
        .map { collection =>
          val fileName = this.fileName(collection)
          val entry = new ZipEntry(s"backup/$fileName")
          out.putNextEntry(entry)
          val count = collection.transaction { implicit transaction =>
            try {
              collection
                .jsonIterator
                .map(JsonFormatter.Compact.apply)
                .map { line =>
                  val bytes = s"$line\n".getBytes("UTF-8")
                  out.write(bytes)
                }
                .size
            } finally {
              out.closeEntry()
            }
          }
          count
        }
        .sum
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
    db
      .collections
      .map(_.asInstanceOf[Collection[KeyValue, KeyValue.type]])
      .map { collection =>
        val fileName = this.fileName(collection)
        val file = new File(directory, fileName)
        val writer = new PrintWriter(file)
        collection.transaction { implicit transaction =>
          collection
            .jsonIterator
            .map(JsonFormatter.Compact.apply)
            .map(writer.println)
            .size
        }
      }
      .sum
  }

  private def fileName(collection: Collection[_, _]): String = s"${collection.name}.jsonl"
}