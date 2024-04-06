package lightdb

import cats.effect.IO
import com.oath.halodb.{HaloDB, HaloDBOptions}
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

case class Store(directory: Path,
                 indexThreads: Int,
                 maxFileSize: Int) {
  private lazy val instance: HaloDB = {
    val opts = new HaloDBOptions
    opts.setBuildIndexThreads(indexThreads)
    opts.setMaxFileSize(maxFileSize)

    Files.createDirectories(directory.getParent)
    HaloDB.open(directory.toAbsolutePath.toString, opts)
  }

  def keyStream[D]: fs2.Stream[IO, Id[D]] = fs2.Stream.fromBlockingIterator[IO](instance.newKeyIterator().asScala, 1024)
    .map { record =>
      Id[D](record.getBytes.string)
    }

  def stream[D]: fs2.Stream[IO, (Id[D], Array[Byte])] = fs2.Stream.fromBlockingIterator[IO](instance.newIterator().asScala, 1024)
    .map { record =>
      val key = record.getKey.string
      Id[D](key) -> record.getValue
    }

  def streamJson[D: RW]: fs2.Stream[IO, D] = stream[D].map {
    case (_, bytes) =>
      val jsonString = bytes.string
      val json = JsonParser(jsonString)
      json.as[D]
  }

  def get[D](id: Id[D]): IO[Option[Array[Byte]]] = IO {
    Option(instance.get(id.bytes))
  }

  def getJson[D: RW](id: Id[D]): IO[Option[D]] = get(id)
    .map(_.map { bytes =>
      val jsonString = bytes.string
      val json = JsonParser(jsonString)
      json.as[D]
    })

  def put[D](id: Id[D], value: Array[Byte]): IO[Boolean] = IO {
    instance.put(id.bytes, value)
  }

  def putJson[D <: Document[D]](doc: D)
                               (implicit rw: RW[D]): IO[D] = IO {
    val json = doc.json
    JsonFormatter.Compact(json)
  }.flatMap { jsonString =>
    put(doc._id, jsonString.getBytes).map(_ => doc)
  }

  def delete[D](id: Id[D]): IO[Unit] = IO {
    instance.delete(id.bytes)
  }

  def size: IO[Int] = IO(instance.size().toInt)

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

  def dispose(): IO[Unit] = IO(instance.close())
}