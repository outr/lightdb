package lightdb.backup

import fabric.rw.*
import fabric.io.JsonParser
import lightdb.LightDB
import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store
import rapid.*

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.zip.ZipFile
import scala.io.{Codec, Source}

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
          Source.fromInputStream(input)(Codec(StandardCharsets.UTF_8))
        }
    }.guarantee(Task(zip.close()))
  }

  def apply(db: LightDB,
            directory: File,
            truncate: Boolean = true): Task[Int] = {
    process(db, truncate) { store =>
      val file = new File(directory, s"${store.name}.jsonl")
      if file.exists() then {
        Some(Source.fromFile(file))
      } else {
        None
      }
    }
  }

  def restore[Doc <: Document[Doc], Model <: DocumentModel[Doc]](store: Store[Doc, Model],
                                                                 file: File,
                                                                 truncate: Boolean = true): Task[Int] = {
    if file.exists() then {
      if truncate then store.transaction(_.truncate).sync()
      val source = Source.fromFile(file)
      val stream = rapid.Stream.fromIterator(Task(source
        .getLines()
        .map(s => JsonParser(s))))
        .map(_.as[Doc](store.model.rw))
      store.transaction { txn =>
        txn.insert(stream)
      }.guarantee(Task(source.close()))
    } else {
      throw new RuntimeException(s"${file.getAbsolutePath} doesn't exist")
    }
  }

  private def process(db: LightDB, truncate: Boolean)(f: Store[_, _ <: DocumentModel[_]] => Option[Source]): Task[Int] = {
    db.stores.map { store =>
      f(store) match {
        case Some(source) =>
          val task = for
            _ <- logger.info(s"Restoring ${store.name}...")
            _ <- store.transaction(_.truncate).when(truncate)
            stream = rapid.Stream.fromIterator(Task(source.getLines().map(JsonParser.apply)))
            count <- store.transaction { txn =>
              txn.insertJson(stream)
            }
            _ <- logger.info(s"Restored $count documents to ${store.name}")
          yield Some((store, count))
          task.guarantee(Task(source.close()))
        case None => Task.pure(None)
      }
    }.tasks.map(_.flatten).flatMap { list =>
      logger.info(s"Finished Restoring ${list.length} stores!").function {
        list.map(_._2).sum
      }
    }
  }
}
