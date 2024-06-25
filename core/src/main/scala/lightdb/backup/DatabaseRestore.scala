package lightdb.backup

import fabric.io.JsonParser
import fabric.rw.Asable
import lightdb.collection.Collection
import lightdb.document.{Document, DocumentModel}
import lightdb.transaction.Transaction
import lightdb.{KeyValue, LightDB}

import java.io.File
import java.util.zip.ZipFile
import scala.io.Source

object DatabaseRestore {
  def archive(db: LightDB,
              archive: File = new File("backup.zip"),
              truncate: Boolean = true): Int = {
    val zip = new ZipFile(archive)
    try {
      db.collections.flatMap { collection =>
        val fileName = s"backup/${collection.name}.jsonl"
        val source = Option(zip.getEntry(fileName))
          .map { zipEntry =>
            val input = zip.getInputStream(zipEntry)
            Source.fromInputStream(input, "UTF-8")
          }
        source.map(s => (collection.asInstanceOf[Collection[KeyValue, KeyValue.type]], s))
      }.map {
        case (collection, source) => collection.transaction { implicit transaction =>
          if (truncate) collection.truncate()
          try {
            restoreStream(collection, source.getLines(), truncate)
          } finally {
            source.close()
          }
        }
      }.sum
    } finally {
      zip.close()
    }
  }

  /**
   * Does a full restore of the supplied database from the directory specified
   */
  def apply(db: LightDB,
            directory: File,
            truncate: Boolean = true): Int = db
    .collections
    .map { collection =>
      val file = new File(directory, s"${collection.name}.jsonl")
      if (file.exists()) {
        restoreCollection(collection.asInstanceOf[Collection[KeyValue, KeyValue.type]], file, truncate)
      } else {
        0
      }
    }
    .sum

  private def restoreStream[D <: Document[D], M <: DocumentModel[D]](collection: Collection[D, M],
                                                                     iterator: Iterator[String],
                                                                     truncate: Boolean = true)
                                                                    (implicit transaction: Transaction[D]): Int = {
    if (truncate) collection.truncate()
    iterator
      .map(JsonParser(_))
      .map(_.as[D](collection.model.rw))
      .map(collection.set(_))
      .size
  }

  /**
   * Restores from a backup of JSON lines.
   */
  private def restoreCollection[D <: Document[D], M <: DocumentModel[D]](collection: Collection[D, M],
                                                                         file: File,
                                                                         truncate: Boolean = true): Int = collection.transaction { implicit transaction =>
    if (truncate) collection.truncate()
    val source = Source.fromFile(file)
    try {
      source.getLines()
        .map(JsonParser(_))
        .map(_.as[D](collection.model.rw))
        .map(collection.set(_))
        .size
    } finally {
      source.close()
    }
  }
}
