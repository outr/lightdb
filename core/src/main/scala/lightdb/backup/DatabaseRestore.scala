package lightdb.backup

import fabric.io.JsonParser
import lightdb.LightDB
import lightdb.collection.Collection

import java.io.File
import java.util.zip.ZipFile
import scala.io.Source

object DatabaseRestore {
  def archive(db: LightDB,
              archive: File = new File("backup.zip"),
              truncate: Boolean = true): Int = {
    val zip = new ZipFile(archive)
    try {
      process(db, truncate) { collection =>
        val fileName = s"backup/${collection.name}.jsonl"
        Option(zip.getEntry(fileName))
          .map { zipEntry =>
            val input = zip.getInputStream(zipEntry)
            Source.fromInputStream(input, "UTF-8")
          }
      }
    } finally {
      zip.close()
    }
  }

  def apply(db: LightDB,
            directory: File,
            truncate: Boolean = true): Int = {
    process(db, truncate) { collection =>
      val file = new File(directory, s"${collection.name}.jsonl")
      if (file.exists()) {
        Some(Source.fromFile(file))
      } else {
        None
      }
    }
  }

  private def process(db: LightDB, truncate: Boolean)(f: Collection[_, _] => Option[Source]): Int = {
    db.collections.map { collection =>
      f(collection) match {
        case Some(source) =>
          try {
            if (truncate) collection.t.truncate()
            val iterator = source
              .getLines()
              .map(s => JsonParser(s))
            collection.t.json.insert(iterator)
          } finally {
            source.close()
          }
        case None => 0
      }
    }.sum
  }
}
