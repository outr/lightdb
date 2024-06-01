package lightdb

import cats.effect.IO
import fabric.Json
import fabric.cryo.Cryo
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._

import java.nio.ByteBuffer

trait Store {
  def keyStream[D]: fs2.Stream[IO, Id[D]]

  def stream[D]: fs2.Stream[IO, (Id[D], Array[Byte])]

  def get[D](id: Id[D]): IO[Option[Array[Byte]]]

  def put[D](id: Id[D], value: Array[Byte]): IO[Boolean]

  def delete[D](id: Id[D]): IO[Unit]

  def size: IO[Int]

  def commit(): IO[Unit]

  def dispose(): IO[Unit]

  def streamJsonDocs[D: RW]: fs2.Stream[IO, D] = stream[D].map {
    case (_, bytes) =>
      val jsonString = bytes.string
      val json = JsonParser(jsonString)
      json.as[D]
  }

  def getJsonDoc[D: RW](id: Id[D]): IO[Option[D]] = get(id)
    .map(_.map { bytes =>
      val json = bytes2Json(bytes)
      json.as[D]
    })

  def putJsonDoc[D <: Document[D]](doc: D)
                                  (implicit rw: RW[D]): IO[D] = IO(doc.json)
    .flatMap(json => putJson(doc._id, json).map(_ => doc))

  def putJson[D <: Document[D]](id: Id[D], json: Json): IO[Unit] =
    IO.blocking(json2Bytes(json)).flatMap(bytes => put(id, bytes)).map(_ => ())

  private def json2Bytes(json: Json): Array[Byte] = JsonFormatter.Compact(json).getBytes

  private def bytes2Json(bytes: Array[Byte]): Json = {
    val jsonString = bytes.string
    JsonParser(jsonString)
  }

//  private def json2Bytes(json: Json): Array[Byte] = Cryo.freeze(json)

//  private def bytes2Json(bytes: Array[Byte]): Json = Cryo.thaw(bytes)

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