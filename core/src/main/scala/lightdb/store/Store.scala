package lightdb.store

import cats.effect.IO
import fabric.Json
import fabric.cryo.Cryo
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import lightdb.Id
import lightdb.document.Document

trait Store {
  def useCryo: Boolean = false

  def keyStream[D]: fs2.Stream[IO, Id[D]]

  def stream: fs2.Stream[IO, Array[Byte]]

  def get[D](id: Id[D]): IO[Option[Array[Byte]]]

  def put[D](id: Id[D], value: Array[Byte]): IO[Boolean]

  def delete[D](id: Id[D]): IO[Unit]

  def count: IO[Int]

  def commit(): IO[Unit]

  def dispose(): IO[Unit]

  def streamJson: fs2.Stream[IO, Json] = stream
    .map { bytes =>
      try {
        bytes2Json(bytes)
      } catch {
        case t: Throwable =>
          throw new RuntimeException(s"Failed to read JSON (${bytes.string}) with ${bytes.length} bytes.", t)
      }
    }

  def streamJsonDocs[D: RW]: fs2.Stream[IO, D] = streamJson.map(_.as[D])

  def getJsonDoc[D: RW](id: Id[D]): IO[Option[D]] = get(id)
    .map(_.map { bytes =>
      try {
        val json = bytes2Json(bytes)
        json.as[D]
      } catch {
        case t: Throwable => throw new RuntimeException(s"Failed to read $id with ${bytes.length} bytes.", t)
      }
    })

  def putJsonDoc[D <: Document[D]](doc: D)
                                  (implicit rw: RW[D]): IO[D] = IO(doc.json)
    .flatMap(json => putJson(doc._id, json).map(_ => doc))

  def putJson[D <: Document[D]](id: Id[D], json: Json): IO[Unit] =
    IO.blocking(json2Bytes(json)).flatMap(bytes => put(id, bytes)).map(_ => ())

  private def json2Bytes(json: Json): Array[Byte] = if (useCryo) {
    Cryo.freeze(json)
  } else {
    JsonFormatter.Compact(json).getBytes
  }

  private def bytes2Json(bytes: Array[Byte]): Json = if (useCryo) {
    Cryo.thaw(bytes)
  } else {
    val jsonString = bytes.string
    JsonParser(jsonString)
  }

  def truncate(): IO[Unit] = keyStream[Any]
    .evalMap { id =>
      delete(id)
    }
    .compile
    .drain
    .flatMap { _ =>
      count.flatMap {
        case 0 => IO.unit
        case _ => truncate()
      }
    }
}