package lightdb

import fabric.Json
import fabric.rw._
import lightdb.doc.{Document, DocumentModel, JsonConversion}

case class KeyValue(_id: Id[KeyValue], value: Json) extends Document[KeyValue]

object KeyValue extends DocumentModel[KeyValue] with JsonConversion[KeyValue] {
  override implicit val rw: RW[KeyValue] = RW.gen

  val value: Field[KeyValue, Json] = field("value", _.value)
}