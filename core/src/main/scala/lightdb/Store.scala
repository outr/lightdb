package lightdb

import cats.effect.IO
import cats.implicits.{catsSyntaxParallelSequence1, toTraverseOps}
import com.oath.halodb.{HaloDB, HaloDBOptions}
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import org.rocksdb.RocksDB

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters._

trait Store {
  def keyStream[D]: fs2.Stream[IO, Id[D]]

  def stream[D]: fs2.Stream[IO, (Id[D], Array[Byte])]

  def get[D](id: Id[D]): IO[Option[Array[Byte]]]

  def put[D](id: Id[D], value: Array[Byte]): IO[Boolean]

  def delete[D](id: Id[D]): IO[Unit]

  def size: IO[Int]

  def dispose(): IO[Unit]

  def streamJson[D: RW]: fs2.Stream[IO, D] = stream[D].map {
    case (_, bytes) =>
      val jsonString = bytes.string
      val json = JsonParser(jsonString)
      json.as[D]
  }

  def getJson[D: RW](id: Id[D]): IO[Option[D]] = get(id)
    .map(_.map { bytes =>
      val jsonString = bytes.string
      val json = JsonParser(jsonString)
      json.as[D]
    })

  def putJson[D <: Document[D]](doc: D)
                               (implicit rw: RW[D]): IO[D] = IO {
    val json = doc.json
    JsonFormatter.Compact(json)
  }.flatMap { jsonString =>
    put(doc._id, jsonString.getBytes).map(_ => doc)
  }

  def truncate(): IO[Unit] = keyStream[Any]
    .evalMap { id =>
      delete(id)
    }
    .compile
    .drain
    .flatMap { _ =>
      size.flatMap {
        case 0 => IO.unit
        case _ => truncate()
      }
    }
}

case class HaloStore(directory: Path,
                 indexThreads: Int,
                 maxFileSize: Int) extends Store {
  private lazy val instance: HaloDB = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)
    opts.setMaxFileSize(maxFileSize)
    opts.setUseMemoryPool(true)
    opts.setMemoryPoolChunkSize(2 * 1024 * 1024)
    opts.setFixedKeySize(32)
    opts.setFlushDataSizeBytes(10 * 1024 * 1024)
    opts.setCompactionThresholdPerFile(0.9)
    opts.setCompactionJobRate(50 * 1024 * 1024)
    opts.setNumberOfRecords(100_000_000)
    opts.setCleanUpTombstonesDuringOpen(true)

    Files.createDirectories(directory.getParent)
    HaloDB.open(directory.toAbsolutePath.toString, opts)
  }

  override def keyStream[D]: fs2.Stream[IO, Id[D]] = fs2.Stream.fromBlockingIterator[IO](instance.newKeyIterator().asScala, 1024)
    .map { record =>
      Id[D](record.getBytes.string)
    }

  override def stream[D]: fs2.Stream[IO, (Id[D], Array[Byte])] = fs2.Stream.fromBlockingIterator[IO](instance.newIterator().asScala, 1024)
    .map { record =>
      val key = record.getKey.string
      Id[D](key) -> record.getValue
    }

  override def get[D](id: Id[D]): IO[Option[Array[Byte]]] = IO {
    Option(instance.get(id.bytes))
  }

  override def put[D](id: Id[D], value: Array[Byte]): IO[Boolean] = IO {
    instance.put(id.bytes, value)
  }

  override def delete[D](id: Id[D]): IO[Unit] = IO {
    instance.delete(id.bytes)
  }

  override def size: IO[Int] = IO(instance.size().toInt)

  override def dispose(): IO[Unit] = IO(instance.close())
}

case class RocksDBStore(directory: Path) extends Store {
  RocksDB.loadLibrary()

  private val db: RocksDB = {
    Files.createDirectories(directory.getParent)
    RocksDB.open(directory.toAbsolutePath.toString)
  }

  override def keyStream[D]: fs2.Stream[IO, Id[D]] = fs2.Stream.force(IO {
    val rocksIterator = db.newIterator()
    rocksIterator.seekToFirst()
    new Iterator[Array[Byte]] {
      override def hasNext: Boolean = rocksIterator.isValid

      override def next(): Array[Byte] = {
        rocksIterator.next()
        rocksIterator.key()
      }
    }
  }.map { iterator =>
    fs2.Stream.fromBlockingIterator[IO](iterator, 1024)
      .map { array =>
        Id[D](array.string)
      }
  })

  override def stream[D]: fs2.Stream[IO, (Id[D], Array[Byte])] = fs2.Stream.force(IO {
    val rocksIterator = db.newIterator()
    rocksIterator.seekToFirst()
    new Iterator[(Array[Byte], Array[Byte])] {
      override def hasNext: Boolean = rocksIterator.isValid

      override def next(): (Array[Byte], Array[Byte]) = {
        rocksIterator.next()
        rocksIterator.key() -> rocksIterator.value()
      }
    }
  }.map { iterator =>
    fs2.Stream.fromBlockingIterator[IO](iterator, 1024)
      .map {
        case (keyArray, valueArray) =>
          Id[D](keyArray.string) -> valueArray
      }
  })

  override def get[D](id: Id[D]): IO[Option[Array[Byte]]] = IO(Option(db.get(id.bytes)))

  override def put[D](id: Id[D], value: Array[Byte]): IO[Boolean] = IO {
    db.put(id.bytes, value)
    true
  }

  override def delete[D](id: Id[D]): IO[Unit] = IO {
    db.delete(id.bytes)
  }

  override def size: IO[Int] = keyStream.compile.count.map(_.toInt)

  override def dispose(): IO[Unit] = IO {
    db.close()
  }
}