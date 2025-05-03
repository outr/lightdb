package lightdb.backup

import fabric.io.JsonParser
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store
import rapid._

import java.io.File
import java.util.zip.ZipFile
import scala.io.Source

object DatabaseRestore {
  def archive(db: LightDB,
              archive: File = new File("backup.zip"),
              truncate: Boolean = true): Task[Int] = {
    val zip = new ZipFile(archive)
    process(db, truncate) { store =>
      val fileName = s"backup/${store.name}.jsonl"
      Option(zip.getEntry(fileName))
        .map { zipEntry =>
          val input = zip.getInputStream(zipEntry)
          Source.fromInputStream(input, "UTF-8")
        }
    }.guarantee(Task(zip.close()))
  }

  def apply(db: LightDB,
            directory: File,
            truncate: Boolean = true): Task[Int] = {
    process(db, truncate) { store =>
      val file = new File(directory, s"${store.name}.jsonl")
      if (file.exists()) {
        Some(Source.fromFile(file))
      } else {
        None
      }
    }
  }

  def restore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: Store[Doc, Model],
                                                                 file: File,
                                                                 truncate: Boolean = true): Task[Int] = {
    if (file.exists()) {
      if (truncate) store.t.truncate()
      val source = Source.fromFile(file)
      val stream = rapid.Stream.fromIterator(Task(source
        .getLines()
        .map(s => JsonParser(s))))
      store.t.json.insert(stream, disableSearchUpdates = false).guarantee(Task(source.close()))
    } else {
      throw new RuntimeException(s"${file.getAbsolutePath} doesn't exist")
    }
  }

  private def process(db: LightDB, truncate: Boolean)(f: Store[_, _ <: DocumentModel[_]] => Option[Source]): Task[Int] = {
    db.stores.map { store =>
      f(store) match {
        case Some(source) =>
          val task = for {
            _ <- logger.info(s"Restoring ${store.name}...")
            _ <- store.t.truncate.when(truncate)
            stream = rapid.Stream.fromIterator(Task(source.getLines().map(JsonParser.apply)))
            count <- store.t.json.insert(stream, disableSearchUpdates = true)
            _ <- logger.info(s"Restored $count documents to ${store.name}")
          } yield Some((store, count))
          task.guarantee(Task(source.close()))
        case None => Task.pure(None)
      }
    }.tasks.map(_.flatten).flatMap { list =>
      for {
        _ <- logger.info("Finished Restoring. Re-Indexing...")
        counts <- list.map {
          case (store, count) => store.reIndex().map(_ => count)
        }.tasksPar
        _ <- logger.info(s"Finished Re-Sync")
      } yield counts.sum
    }
  }
}
