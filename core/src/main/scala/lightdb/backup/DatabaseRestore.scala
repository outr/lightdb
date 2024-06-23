package lightdb.backup

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeByName, toTraverseOps}
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
              truncate: Boolean = true): IO[Int] = {
    val zip = new ZipFile(archive)
    fs2.Stream(db.collections: _*)
      .map { collection =>
        val fileName = s"backup/${collection.name}.jsonl"
        val source = Option(zip.getEntry(fileName))
          .map { zipEntry =>
            val input = zip.getInputStream(zipEntry)
            Source.fromInputStream(input, "UTF-8")
          }
        source.map(s => (collection.asInstanceOf[Collection[KeyValue, KeyValue.type]], s))
      }
      .unNone
      .evalMap {
        case (collection, source) => collection.transaction { implicit transaction =>
          collection.truncate().whenA(truncate).flatMap { _ =>
            val stream = fs2.Stream.fromBlockingIterator[IO](source.getLines(), 512)
            restoreStream(collection, stream, truncate).guarantee(IO(source.close()))
          }
        }
      }
      .compile
      .toList
      .map(_.sum)
      .guarantee(IO(zip.close()))
  }

  /**
   * Does a full restore of the supplied database from the directory specified
   */
  def apply(db: LightDB,
            directory: File,
            truncate: Boolean = true): IO[Int] = db
    .collections
    .map { collection =>
      val file = new File(directory, s"${collection.name}.jsonl")
      if (file.exists()) {
        restoreCollection(collection.asInstanceOf[Collection[KeyValue, KeyValue.type]], file, truncate)
      } else {
        IO.pure(0)
      }
    }
    .sequence
    .map(_.sum)

  private def restoreStream[D <: Document[D], M <: DocumentModel[D]](collection: Collection[D, M],
                                                                  stream: fs2.Stream[IO, String],
                                                                  truncate: Boolean = true)
                                                                    (implicit transaction: Transaction[D]): IO[Int] = {
    collection.truncate().whenA(truncate).flatMap { _ =>
      stream
        .map(JsonParser(_))
        .map(_.as[D](collection.model.rw))
        .evalMap(collection.set(_))
        .compile
        .count
        .map(_.toInt)
    }
  }

  /**
   * Restores from a backup of JSON lines.
   */
  private def restoreCollection[D <: Document[D], M <: DocumentModel[D]](collection: Collection[D, M],
                                                  file: File,
                                                  truncate: Boolean = true): IO[Int] = collection.transaction { implicit transaction =>
    collection.truncate().whenA(truncate).flatMap { _ =>
      val source = Source.fromFile(file)
      fs2.Stream
        .fromBlockingIterator[IO](source.getLines(), 512)
        .map(JsonParser(_))
        .map(_.as[D](collection.model.rw))
        .evalMap(collection.set(_))
        .compile
        .count
        .map(_.toInt)
        .guarantee(IO(source.close()))
    }
  }
}
