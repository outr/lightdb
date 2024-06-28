package lightdb.store

import fabric.io.{JsonFormatter, JsonParser}
import fabric.rw.{Asable, Convertible}
import lightdb.document.Document

trait StringStore[D <: Document[D]] extends Store[D] {
  protected def string2D(string: String): D = {
    val json = JsonParser(string)
    json.as[D]
  }

  protected def d2String(doc: D): String = {
    val json = doc.json
    JsonFormatter.Compact(json)
  }
}
