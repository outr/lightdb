package lightdb.rocks

import cats.effect.IO
import lightdb.{Id, Store}
import org.rocksdb.{RocksDB, RocksIterator}

import java.nio.file.{Files, Path}
import scala.collection.mutable.ListBuffer
import lightdb._

case class RocksDBStore(directory: Path) extends Store {
  RocksDB.loadLibrary()

  private val db: RocksDB = {
    Files.createDirectories(directory.getParent)
    RocksDB.open(directory.toAbsolutePath.toString)
  }

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

  override def keyStream[D]: fs2.Stream[IO, Id[D]] = createStream { i =>
    Option(i.key()).map(key => Id[D](key.string))
  }

  override def stream[D]: fs2.Stream[IO, (Id[D], Array[Byte])] = createStream { i =>
    Option(i.key()).map(key => Id[D](key.string) -> i.value())
  }

  override def get[D](id: Id[D]): IO[Option[Array[Byte]]] = IO.blocking(Option(db.get(id.bytes)))

  override def put[D](id: Id[D], value: Array[Byte]): IO[Boolean] = IO.blocking {
    db.put(id.bytes, value)
    true
  }

  override def delete[D](id: Id[D]): IO[Unit] = IO.blocking {
    db.delete(id.bytes)
  }

  override def size: IO[Int] = keyStream.compile.count.map(_.toInt)

  override def commit(): IO[Unit] = IO.unit

  override def dispose(): IO[Unit] = IO.blocking {
    db.close()
  }
}
