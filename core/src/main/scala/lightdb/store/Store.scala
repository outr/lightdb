package lightdb.store

import fabric.Json
import fabric.cryo.Cryo
import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw._
import lightdb.Id
import lightdb.document.Document

import scala.annotation.tailrec

trait Store[D] {
  implicit val rw: RW[D]

//  def useCryo: Boolean = false

  def idIterator: Iterator[Id[D]]

  def iterator: Iterator[D]

  def get(id: Id[D]): Option[D]

  def put(id: Id[D], doc: D): Boolean

  def delete(id: Id[D]): Unit

  def count: Int

  def commit(): Unit

  def dispose(): Unit

//  def streamJson: Iterator[Json] = stream
//    .map { bytes =>
//      try {
//        bytes2Json(bytes)
//      } catch {
//        case t: Throwable =>
//          throw new RuntimeException(s"Failed to read JSON (${bytes.string}) with ${bytes.length} bytes.", t)
//      }
//    }
//
//  def getJsonDoc[D: RW](id: Id[D]): Option[D] = get(id)
//    .map { bytes =>
//      try {
//        val json = bytes2Json(bytes)
//        json.as[D]
//      } catch {
//        case t: Throwable => throw new RuntimeException(s"Failed to read $id with ${bytes.length} bytes.", t)
//      }
//    }
//
//  def putJsonDoc[D <: Document[D]](doc: D)
//                                  (implicit rw: RW[D]): D = {
//    putJson(doc._id, doc.json)
//    doc
//  }
//
//  def putJson[D <: Document[D]](id: Id[D], json: Json): Unit = {
//    val bytes = json2Bytes(json)
//    put(id, bytes)
//  }
//
//  private def json2Bytes(json: Json): Array[Byte] = if (useCryo) {
//    Cryo.freeze(json)
//  } else {
//    JsonFormatter.Compact(json).getBytes
//  }
//
//  private def bytes2Json(bytes: Array[Byte]): Json = if (useCryo) {
//    Cryo.thaw(bytes)
//  } else {
//    val jsonString = bytes.string
//    JsonParser(jsonString)
//  }

  def truncate(): Unit = internalTruncate()

  @tailrec
  final def internalTruncate(): Unit = {
    idIterator.foreach(delete)
    if (count > 0) {
      internalTruncate()
    }
  }
}