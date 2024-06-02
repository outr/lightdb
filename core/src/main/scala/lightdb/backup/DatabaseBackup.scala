package lightdb.backup

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeByName, toTraverseOps}
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.Asable
import lightdb.{Document, KeyValue, LightDB}
import lightdb.model.AbstractCollection

import java.io.{File, PrintWriter}
import scala.io.Source

object DatabaseBackup {
  /**
   * Does a full backup of the supplied database to the directory specified
   */
  def backup(db: LightDB, directory: File): IO[Int] = {
    directory.mkdirs()
    db
      .collections
      .map { collection =>
        backupCollection(collection.asInstanceOf[AbstractCollection[KeyValue]], new File(directory, s"${collection.collectionName}.jsonl"))
      }
      .sequence
      .map(_.sum)
  }

  /**
   * Does a full restore of the supplied database from the directory specified
   */
  def restore(db: LightDB,
              directory: File,
              truncate: Boolean = true): IO[Int] = db
    .collections
    .map { collection =>
      val file = new File(directory, s"${collection.collectionName}.jsonl")
      if (file.exists()) {
        restoreCollection(collection.asInstanceOf[AbstractCollection[KeyValue]], file, truncate).flatTap { _ =>
          collection.reIndex()
        }
      } else {
        IO.pure(0)
      }
    }
    .sequence
    .map(_.sum)

  /**
   * Creates a backup of the supplied collection to the supplied file as JSON lines.
   */
  def backupCollection[D <: Document[D]](collection: AbstractCollection[D], file: File): IO[Int] = {
    val writer = new PrintWriter(file)
    collection
      .jsonStream
      .map(JsonFormatter.Compact(_))
      .map(writer.println)
      .compile
      .count
      .map(_.toInt)
      .guarantee(IO {
        writer.flush()
        writer.close()
      })
  }

  /**
   * Restores from a backup of JSON lines.
   */
  def restoreCollection[D <: Document[D]](collection: AbstractCollection[D],
                                file: File,
                                truncate: Boolean = true): IO[Int] = collection.truncate().whenA(truncate).flatMap { _ =>
    val source = Source.fromFile(file)
    fs2.Stream
      .fromBlockingIterator[IO](source.getLines(), 512)
      .map(JsonParser(_))
      .map(_.as[D](collection.rw))
      .evalMap(collection.set(_))
      .compile
      .count
      .map(_.toInt)
      .guarantee(IO(source.close()))
  }
}