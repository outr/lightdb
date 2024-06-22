package lightdb.rocks

import cats.effect.IO
import fabric.Json
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.RW
import lightdb.Id
import org.rocksdb.{RocksDB, RocksIterator}

import java.nio.file.{Files, Path}
import lightdb.document.Document
import lightdb.store.{JsonStore, Store, StoreManager}
import lightdb.transaction.Transaction

case class RocksDBStore[D <: Document[D]](directory: Path)
                                         (implicit val rw: RW[D]) extends JsonStore[D] {
  override type Serialized = Array[Byte]

  private lazy val db: RocksDB = {
    Files.createDirectories(directory.getParent)
    RocksDB.open(directory.toAbsolutePath.toString)
  }

  override protected def initialize(): IO[Unit] = IO {
    RocksDB.loadLibrary()
    db
  }

  override protected def json2Serialized(json: Json): IO[Array[Byte]] =
    IO(JsonFormatter.Compact(json).getBytes("UTF-8"))

  override protected def serialized2Json(serialized: Array[Byte]): IO[Json] = IO {
    val jsonString = new String(serialized, "UTF-8")
    JsonParser(jsonString)
  }

  override protected def setSerialized(id: Id[D],
                                       serialized: Array[Byte],
                                       transaction: Transaction[D]): IO[Boolean] = IO {
    db.put(id.bytes, serialized)
    true
  }

  override protected def getSerialized(id: Id[D],
                                       transaction: Transaction[D]): IO[Option[Array[Byte]]] =
    IO.blocking(Option(db.get(id.bytes)))

  override protected def streamSerialized(transaction: Transaction[D]): fs2.Stream[IO, Array[Byte]] = createStream { i =>
    Option(i.value())
  }

  override def idStream(implicit transaction: Transaction[D]): fs2.Stream[IO, Id[D]] = createStream { i =>
    Option(i.key()).map(key => Id[D](new String(key, "UTF-8")))
  }

  override def count(implicit transaction: Transaction[D]): IO[Int] = idStream.compile.count.map(_.toInt)

  override def delete(id: Id[D])(implicit transaction: Transaction[D]): IO[Boolean] = IO.blocking {
    db.delete(id.bytes)
    true
  }

  override def truncate()(implicit transaction: Transaction[D]): IO[Int] = idStream
    .evalMap(delete)
    .compile
    .count
    .map(_.toInt)

  private def createStream[T](f: RocksIterator => Option[T]): fs2.Stream[IO, T] = {
    val io = IO.blocking {
      val rocksIterator = db.newIterator()
      rocksIterator.seekToFirst()
      val iterator = new Iterator[Option[T]] {
        override def hasNext: Boolean = rocksIterator.isValid

        override def next(): Option[T] = try {
          f(rocksIterator)
        } finally {
          rocksIterator.next()
        }
      }
      fs2.Stream.fromBlockingIterator[IO](iterator, 512)
        .unNoneTerminate
    }
    fs2.Stream.force(io)
  }

  override def dispose(): IO[Unit] = IO.blocking {
    db.close()
  }
}

object RocksDBStore extends StoreManager {
  override protected def create[D <: Document[D]](name: String)
                                                 (implicit rw: RW[D]): IO[Store[D]] = IO {
    // TODO: Fix path resolution
    new RocksDBStore[D](Path.of("db", name))
  }
}