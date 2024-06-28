package lightdb.store

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.document.Document

trait ByteStore[D <: Document[D]] extends Store[D] {
  protected def bytes2D(bytes: Array[Byte]): D = {
    val jsonString = bytes.string
    val json = JsonParser(jsonString)
    json.as[D]
  }

  protected def d2Bytes(doc: D): Array[Byte] = {
    val json = doc.json
    val jsonString = JsonFormatter.Compact(json)
    jsonString.getBytes("UTF-8")
  }
}